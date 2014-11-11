import datetime
import os
import sys
import time
import shutil

try:  # Import Python 3 package, turn back to Python 2 if fails
    import configparser
except ImportError:
    import ConfigParser as configparser

from threading import RLock

from execo.action import TaktukPut
from execo.time_utils import timedelta_to_seconds, format_date, get_seconds
from execo_engine import logger
from execo_engine.engine import Engine
from execo_engine.sweep import ParamSweeper, sweep
from execo_g5k.api_utils import get_cluster_site
from execo_g5k.kadeploy import Deployment, deploy
from execo_g5k.oar import oarsub, get_oar_job_nodes, get_oar_job_info, oardel
from execo_g5k.planning import get_jobs_specs, get_planning, compute_slots

from div_p2p.test_thread import TestThread


class DivEngineException(Exception):
    pass


class ParameterException(DivEngineException):
    pass


class StatsManager(object):
    """This class manages the statistics of the tests. It is thread-safe."""

    def __init__(self, engine):
        """Create a StatsManager linked to the given engine.

        Args:
          engine (DivEngine): The engine to which the StatsManager is linked to.
        """

        self.__lock = RLock()
        self.engine = engine

        self.stats_path = ""
        self.remove_output = True
        self.output_path = None
        self.summary_file_name = "summary.csv"
        self.ds_summary_file_name = "ds-summary.csv"
        self.summary_file = None
        self.ds_summary_file = None

        self.summary_props = []

        self.printed_dss = []

    def initialize(self, ds_parameters, xp_parameters):
        """Create and write headers of the summary files.

        Args:
          ds_parameters (dict): The datasets parameters.
          xp_parameters (dict): The experiments parameters.
        """

        with self.__lock:
            # Xp summary
            self.summary_file = open(self.summary_file_name, "w")
            self.summary_props = []
            self.summary_props.extend(ds_parameters.keys())
            self.summary_props.extend(xp_parameters.keys())
            header = "comb_id"
            for pn in self.summary_props:
                header += ", " + str(pn)
            self.summary_file.write(header + "\n")
            self.summary_file.flush()

            # Ds summary
            self.ds_summary_file = open(self.ds_summary_file_name, "w")
            header = "ds_id, ds_class, ds_class_properties"
            self.ds_summary_file.write(header + "\n")
            self.ds_summary_file.flush()

    def add_ds(self, ds_id, comb):
        """Add a new dataset to the statistics.

        Args:
          ds_id (int): The dataset combination identifier.
          comb (dict): The combination including the dataset's parameters.
        """

        if not ds_id in self.printed_dss:

            (ds_class_name, ds_params) = \
                self.engine.comb_manager.get_ds_class_params(comb)

            line = str(ds_id) + "," + ds_class_name + "," + str(ds_params)

            with self.__lock:
                if not ds_id in self.printed_dss:
                    self.ds_summary_file.write(line + "\n")
                    self.ds_summary_file.flush()

                    self.printed_dss.append(ds_id)

    def add_xp(self, comb_id, comb, out_path):
        """Add a new experiment to the statistics.

        Args:
          comb_id (int): The experiment combination identifier.
          comb (dict): The combination including the experiment's parameters.
        """

        local_path = os.path.join(self.stats_path, str(comb_id))

        logger.info("Copying stats from comb with id " + str(comb_id) +
                    " to " + local_path)
        shutil.copyfile(out_path, local_path)

        line = str(comb_id)
        for pn in self.summary_props:
            line += ", " + str(comb[pn])

        with self.__lock:
            self.summary_file.write(line + "\n")
            self.summary_file.flush()

    def close(self):
        """Close the summary files."""

        with self.__lock:
            if self.summary_file:
                self.summary_file.close()
            if self.ds_summary_file:
                self.ds_summary_file.close()


class CombinationManager(object):
    """This class manages the combination of the tests. It is thread-safe in the
    creation of the identifiers. Its sweeper is itself thread-safe."""

    def __init__(self, engine):
        """Create a CombinationManager linked to the given engine.

        Args:
          engine (DivEngine): The engine to which the CombinationManager is
            linked to.
        """

        self.__lock = RLock()
        self.engine = engine
        self.comb_id = 0
        self.ds_id = 0

        self.num_repetitions = 1

    def get_ds_class_params(self, comb):
        """Return the dataset class parameters for the given combination.

        Args:
          comb (dict): The dataset parameters.

        Returns:
          dict: the dataset class parameters.
        """
        ds_idx = comb["ds.config"]
        return self.engine.ds_config[ds_idx]

    def get_comb_id(self, comb):
        """Generate a new experiment identifier for the given combination.

        Args:
          comb (dict): The experiment parameters.

        Returns:
          int: the combination identifier.
        """
        with self.__lock:
            comb_id = self.comb_id
            self.comb_id += 1
        return comb_id

    def get_ds_id(self, comb):
        """Generate a new dataset identifier for the given combination.

        Args:
          comb (dict): The dataset parameters.

        Returns:
          int: the combination identifier.
        """
        return comb["ds.config"]

    def get_ds_parameters(self, params):
        """Return the params and values referring to the dataset.

        Args:
          params (dict): The parameters.

        Returns:
          dict: The params referring to the dataset.
        """
        ds_params = {}
        for pn in self.engine.ds_parameters:
            ds_params[pn] = params[pn]
        ds_params["ds.config"] = self.engine.ds_config[params["ds.config"]]
        return ds_params

    def get_xp_parameters(self, params):
        """Return the params and values referring to the experiment.

        Args:
          params (dict): The parameters.

        Returns:
          dict: The params referring to the experiment.
        """
        xp_params = {}
        for pn in self.engine.xp_parameters:
            xp_params[pn] = params[pn]
        return xp_params

    def get_num_repetitions(self):
        """Return the number of repetitions to be performed for each
        combination.

        Returns:
          int: the number of repetitions.
        """
        return self.num_repetitions

    def uses_same_ds(self, comb1, comb2):
        """Determine if both combinations use the same dataset.

        Args:
          comb1 (dict): The first combination.
          comb2 (dict): The second combination.
        """

        for var in self.engine.ds_parameters.keys():
            if comb1[var] != comb2[var]:
                return False
        return True


class DivEngine(Engine):
    """This class manages thw whole workflow of a div_p2p test suite."""

    def __init__(self):
        self.frontend = None
        super(DivEngine, self).__init__()

        # Parameter definition
        self.options_parser.set_usage(
            "usage: %prog <cluster> <n_nodes> <config_file>")
        self.options_parser.add_argument("cluster",
                    "The cluster on which to run the experiment")
        self.options_parser.add_argument("n_nodes",
                    "The number of nodes in which the experiment is going to be"
                    " deployed")
        self.options_parser.add_argument("config_file",
                    "The path of the file containing test params (INI file)")
        self.options_parser.add_option("-k", dest="keep_alive",
                    help="keep reservation alive ..",
                    action="store_true")
        self.options_parser.add_option("-j", dest="oar_job_id",
                    help="oar_job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_option("-o", dest="outofchart",
                    help="Run the engine outside days",
                    action="store_true")
        self.options_parser.add_option("-w", dest="walltime",
                    help="walltime for the reservation",
                    type="string",
                    default="1:00:00")

        # Configuration variables
        self.ds_id = 0

        self.stats_manager = StatsManager(self)
        self.comb_manager = CombinationManager(self)

        self.use_kadeploy = False
        self.kadeploy_env_file = None
        self.kadeploy_env_name = None

        self.jar_file = None
        self.remote_dir = "/tmp"

    def run(self):
        """Inherited method, put here the code for running the engine."""

        # Get parameters
        self.cluster = self.args[0]
        self.n_nodes = int(self.args[1])
        self.config_file = self.args[2]
        self.site = get_cluster_site(self.cluster)

        if not os.path.exists(self.config_file):
            logger.error("Params file " + self.config_file + " does not exist")
            sys.exit(1)

        # Set oar job id
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        # Main
        try:
            # Creation of the main iterator used for the first control loop.
            self.define_parameters()

            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:

                ## SETUP
                # If no job, we make a reservation and prepare the hosts for the
                # experiments
                if job_is_dead or self.oar_job_id is None:
                    self.make_reservation()
                    success = self.setup()
                    if not success:
                        break
                else:
                    self.hosts = get_oar_job_nodes(self.oar_job_id,
                                                   self.frontend)
                ## SETUP FINISHED

                logger.info("Setup finished in hosts " + str(self.hosts))

                test_threads = []
                for h in self.hosts:
                    t = TestThread(h, self.comb_manager, self.stats_manager)
                    test_threads.append(t)
                    t.name = "th_" + str(h.address).split(".")[0]
                    t.start()

                for t in test_threads:
                    t.join()

                if get_oar_job_info(self.oar_job_id,
                                    self.frontend)['state'] == 'Error':
                    job_is_dead = True

        finally:
            if self.oar_job_id is not None:
                if not self.options.keep_alive:
                    pass
                    logger.info('Deleting job')
                    oardel([(self.oar_job_id, self.frontend)])
                else:
                    logger.info('Keeping job alive for debugging')

            # Close stats
            self.stats_manager.close()

    def __define_test_parameters(self, config):
        if config.has_section("test_parameters"):
            test_parameters_names = config.options("test_parameters")
            if "test.stats_path" in test_parameters_names:
                self.stats_manager.stats_path = \
                    config.get("test_parameters", "test.stats_path")
                if not os.path.exists(self.stats_manager.stats_path):
                    os.makedirs(self.stats_manager.stats_path)

            if "test.summary_file" in test_parameters_names:
                self.stats_manager.summary_file_name = \
                    config.get("test_parameters", "test.summary_file")

            if "test.ds_summary_file" in test_parameters_names:
                self.stats_manager.ds_summary_file_name = \
                    config.get("test_parameters", "test.ds_summary_file")

            if "test.num_repetitions" in test_parameters_names:
                self.comb_manager.num_repetitions = \
                    int(config.get("test_parameters", "test.num_repetitions"))

            if "test.jar_file" in test_parameters_names:
                self.jar_file = config.get("test_parameters", "test.jar_file")

            if "test.remote_dir" in test_parameters_names:
                self.remote_dir = config.get("test_parameters",
                                             "test.remote_dir")

            if "test.use_kadeploy" in test_parameters_names:
                self.use_kadeploy = config.getboolean("test_parameters",
                                                      "test.use_kadeploy")

            if self.use_kadeploy:
                if "test.kadeploy.env_file" in test_parameters_names:
                    self.kadeploy_env_file = \
                        config.get("test_parameters", "test.kadeploy.env_file")
                elif "test.kadeploy.env_name" in test_parameters_names:
                    self.kadeploy_env_name = \
                        config.get("test_parameters", "test.kadeploy.env_name")
                else:
                    logger.error("Either test.kadeploy.env_file or "
                                 "test.kadeploy.env_name should be specified")
                    raise ParameterException("Either test.kadeploy.env_file or "
                                             "test.kadeploy.env_name should be "
                                             "specified")

    def __define_ds_parameters(self, config):
        ds_parameters_names = config.options("ds_parameters")
        self.ds_parameters = {}
        ds_class_parameters = {}
        ds_classes = []
        for pn in ds_parameters_names:
            pv = config.get("ds_parameters", pn).split(",")
            if pn.startswith("ds.class."):
                ds_class_parameters[pn[len("ds.class."):]] = \
                    [v.strip() for v in pv]
            elif pn == "ds.class":
                ds_classes = [v.strip() for v in pv]
            else:
                self.ds_parameters[pn] = [v.strip() for v in pv]

        # Create ds configurations
        self.ds_config = []
        for (idx, ds_class) in enumerate(ds_classes):
            this_ds_params = {}
            for pn, pv in ds_class_parameters.iteritems():
                if len(pv) == len(ds_classes):
                    if pv[idx]:
                        this_ds_params[pn] = pv[idx]
                elif len(pv) == 1:
                    this_ds_params[pn] = pv[0]
                else:
                    logger.error("Number of ds_class does not much number of " +
                                 pn)
                    raise ParameterException("Number of ds_class does not much "
                                             "number of " + pn)

            self.ds_config.append((ds_class, this_ds_params))

        self.ds_parameters["ds.config"] = range(0, len(self.ds_config))

    def define_parameters(self):
        """Create the iterator that contains the parameters to be explored."""

        config = configparser.ConfigParser()
        config.readfp(open(self.config_file))

        # TEST PARAMETERS
        self.__define_test_parameters(config)

        # DATASET PARAMETERS
        self.__define_ds_parameters(config)

        # EXPERIMENT PARAMETERS
        xp_parameters_names = config.options("xp_parameters")
        self.xp_parameters = {}
        for pn in xp_parameters_names:
            pv = config.get("xp_parameters", pn).split(",")
            self.xp_parameters[pn] = [v.strip() for v in pv]

        # GLOBAL
        self.parameters = {}
        self.parameters.update(self.ds_parameters)
        self.parameters.update(self.xp_parameters)

        # SUMMARY FILES
        self.stats_manager.initialize(self.ds_parameters, self.xp_parameters)

        # PRINT PARAMETERS
        print_ds_parameters = {}
        print_ds_parameters.update(self.ds_parameters)
        print_ds_parameters["ds.config"] = self.ds_config
        logger.info("Dataset parameters: " + str(print_ds_parameters))
        logger.info("Experiment parameters: " + str(self.xp_parameters))

        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"),
                                    sweep(self.parameters))
        self.comb_manager.sweeper = self.sweeper

        logger.info('Number of parameters combinations %s, '
                    'Number of repetitions %s',
                    len(self.sweeper.get_remaining()),
                    self.comb_manager.num_repetitions)

    def make_reservation(self):
        """Perform a reservation of the required number of nodes."""

        logger.info('Performing reservation')
        now = int(time.time() +
                  timedelta_to_seconds(datetime.timedelta(minutes=1)))
        starttime = now
        endtime = int(starttime +
                      timedelta_to_seconds(datetime.timedelta(days=3,
                                                              minutes=1)))
        startdate, n_nodes = self._get_nodes(starttime, endtime)

        search_time = 3 * 24 * 60 * 60  # 3 days
        walltime_seconds = get_seconds(self.options.walltime)

        iteration = 0
        while not n_nodes:
            iteration += 1
            logger.info('Not enough nodes found between %s and %s, ' +
                        'increasing time window',
                        format_date(starttime), format_date(endtime))
            starttime = max(now, now +
                            iteration * search_time - walltime_seconds)
            endtime = int(now + (iteration + 1) * search_time)

            startdate, n_nodes = self._get_nodes(starttime, endtime)
            if starttime > int(time.time() + timedelta_to_seconds(
                    datetime.timedelta(weeks=6))):
                logger.error('There are not enough nodes on %s for your ' +
                             'experiments, abort ...', self.cluster)
                exit()

        jobs_specs = get_jobs_specs({self.cluster: n_nodes},
                                    name=self.__class__.__name__)
        sub = jobs_specs[0][0]
        sub.walltime = self.options.walltime
        if self.use_kadeploy:
            sub.additional_options = '-t deploy'
        else:
            sub.additional_options = '-t allow_classic_ssh'
        sub.reservation_date = startdate
        (self.oar_job_id, self.frontend) = oarsub(jobs_specs)[0]
        logger.info('Startdate: %s, n_nodes: %s, job_id: %s',
                    format_date(startdate),
                    str(n_nodes), str(self.oar_job_id))

    def _get_nodes(self, starttime, endtime):

        planning = get_planning(elements=[self.cluster],
                                starttime=starttime,
                                endtime=endtime,
                                out_of_chart=self.options.outofchart)
        slots = compute_slots(planning, self.options.walltime)
        startdate = slots[0][0]
        i_slot = 0
        n_nodes = slots[i_slot][2][self.cluster]
        while n_nodes < self.n_nodes:
            logger.debug(slots[i_slot])
            startdate = slots[i_slot][0]
            n_nodes = slots[i_slot][2][self.cluster]
            i_slot += 1
            if i_slot == len(slots) - 1:
                return False, False
        return startdate, self.n_nodes

    def setup(self):
        """Setup the cluster of hosts. Optionally deploy env and then copy the
        executable jar to all the nodes.
        """

        self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)

        if self.use_kadeploy:
            (deployed, undeployed) = self.deploy_nodes()
            return (len(deployed) != 0)

        copy_code = TaktukPut(self.hosts, [self.jar_file], self.remote_dir)
        copy_code.run()

        return True

    def deploy_nodes(self, min_deployed_hosts=1, max_tries=3):
        """Deploy nodes in the cluster. If the number of deployed nodes is less
        that the specified min, try again.

        Args:
          min_deployed_hosts (int, optional): minimum number of nodes to be
            deployed (default: 1).
          max_tries (int, optional): maximum number of tries to reach the
            minimum number of nodes (default: 3).
        """

        logger.info("Deploying " + str(len(self.hosts)) + " nodes")

        def correct_deployment(deployed, undeployed):
            return len(deployed) >= min_deployed_hosts

        if self.kadeploy_env_file:
            deployment = Deployment(self.hosts, env_file=self.kadeploy_env_file)
        elif self.kadeploy_env_name:
            deployment = Deployment(self.hosts, env_name=self.kadeploy_env_name)
        else:
            logger.error("Neither env_file nor env_name are specified")
            raise ParameterException("Neither env_file nor env_name are "
                                     "specified")

        (deployed, undeployed) = deploy(
            deployment,
            num_tries=max_tries,
            check_enough_func=correct_deployment,
            out=True
        )

        logger.info("%i deployed, %i undeployed" % (len(deployed),
                                                    len(undeployed)))

        if not correct_deployment(deployed, undeployed):
            logger.error("It was not possible to deploy min number of hosts")

        return (deployed, undeployed)
