import os
import tempfile

from execo.action import Put
from execo.process import SshProcess


class DivP2PWrapper:

    def __init__(self, host, remote_dir="/tmp", jar_path="/tmp/diversity_p2p.jar"):
        self.host = host
        self.remote_dir = remote_dir
        self.jar_path = jar_path

        self.props_path = os.path.join(self.remote_dir, "properties.dat")

    def change_conf(self, params):
        """Create a new properties file from configuration and transfer it to
        the host.

        Args:
          params (dict): The parameters of the test.
        """

        # Create a local temporary file with the params
        (_, temp_file) = tempfile.mkstemp("", "div_p2p-conf-", "/tmp")
        props = open(temp_file, "w")
        for key in params:
            props.write(str(key) + "=" + str(params[key]) + "\n")
        props.close()

        # Copy the file to the remote location
        copy_props = Put([self.host], [temp_file], self.props_path)
        copy_props.run()

        # Remove temporary file
        os.remove(temp_file)

    def execute(self):
        """Execute a single test.

        Return:
          str: Local path of the file containing the process output.
        """

        test = SshProcess("java -jar " + self.jar_path +
                          " -p " + self.props_path,
                          self.host)

        # Output is stored in a local temporary file
        (_, temp_file) = tempfile.mkstemp("", "div_p2p-out-", "/tmp")
        test.stdout_handlers.append(temp_file)

        test.run()

        return temp_file
