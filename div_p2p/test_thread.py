import os
from threading import Thread
from execo.action import TaktukPut
from execo.log import style
from execo_engine import logger
from div_p2p.wrapper import DivP2PWrapper


class TestThread(Thread):

    def __init__(self, host, comb_manager, stats_manager):
        super(TestThread, self).__init__()

        self.div_p2p = DivP2PWrapper(host)

        self.comb_manager = comb_manager
        self.stats_manager = stats_manager

    def _th_prefix(self):
        return style.user1("[" + self.name + "] ")

    def run(self):

        while len(self.comb_manager.sweeper.get_remaining()) > 0:

            # Getting the next combination (which uses a new dataset)
            comb = self.comb_manager.sweeper.get_next()

            if comb:
                self.comb = comb
                self.comb_id = self.comb_manager.get_comb_id(comb)

                self.ds_id = self.comb_manager.get_ds_id(comb)
                ds_comb = self.prepare_dataset(comb)

                self.xp(comb, ds_comb)

                # subloop over the combinations that use the same dataset
                while True:
                    comb_in_ds = self.comb_manager.sweeper.get_next(
                        lambda r: filter(self._uses_same_ds, r))

                    if comb_in_ds:
                        self.comb = comb
                        self.comb_id = self.comb_manager.get_comb_id(comb)
                        try:
                            self.xp(comb_in_ds, ds_comb)
                        except:
                            break
                    else:
                        break

    def _uses_same_ds(self, candidate_comb):
        """Determine if the candidate combination uses the same dataset as the
        current one.

        Args:
          candidate_comb (dict): The combination candidate to be selected as the
            new combination.
        """

        return self.comb_manager.uses_same_ds(self.comb, candidate_comb)

    def prepare_dataset(self, comb):
        """Prepare the dataset to be used in the next set of experiments.

        Args:
          comb (dict): The combination containing the dataset's parameters.

        Returns:
          dict: The dataset parameters.

        """

        # Create ds_comb
        (ds_class_name, ds_params) = self.comb_manager.get_ds_class_params(comb)

        local_path = ds_params["local_path"]
        remote_path = os.path.join(self.div_p2p.remote_dir, os.path.basename(local_path))

        ds_comb = {}
        ds_comb["ds.class.path"] = remote_path
        ds_comb["ds.class"] = ds_class_name

        # Copy dataset to host
        logger.info(self._th_prefix() + "Prepare dataset with combination " +
                    str(self.comb_manager.get_ds_parameters(comb)))

        copy_code = TaktukPut([self.div_p2p.host], [local_path], remote_path)
        copy_code.run()

        # Notify stats manager
        self.stats_manager.add_ds(self.ds_id, comb)

        return ds_comb

    def xp(self, comb, ds_comb):
        """Perform the experiment corresponding to the given combination.

        Args:
          comb (dict): The combination with the experiment's parameters.
          ds_comb (dict): The dataset parameters.
        """

        comb_ok = False
        try:
            logger.info(self._th_prefix() + "Execute experiment with combination " +
                        str(self.comb_manager.get_xp_parameters(comb)))

            num_reps = self.comb_manager.get_num_repetitions()
            for nr in range(0, num_reps):

                if num_reps > 1:
                    logger.info(self._th_prefix() + "Repetition " + str(nr + 1))

                # Change configuration
                params = {}
                for key in comb:
                    params[key] = comb[key]
                for key in ds_comb:
                    params[key] = ds_comb[key]
                self.div_p2p.change_conf(params)

                # Execute job
                stats_file = self.div_p2p.execute()

                # Notify stats manager
                self.stats_manager.add_combination(self.comb_id, comb, stats_file)

            comb_ok = True

        finally:
            if comb_ok:
                self.comb_manager.sweeper.done(comb)
            else:
                self.comb_manager.sweeper.cancel(comb)
            logger.info('%s Remaining', len(self.comb_manager.sweeper.get_remaining()))