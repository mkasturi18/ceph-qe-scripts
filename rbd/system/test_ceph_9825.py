# Script to execute the test CEPH 9825
#  Test Description: Create image with Deep Flattening enabled,take snap,protect,
#                    clone,snap,flatten the clone, unprotect the parent snap, delete the parent snap
#  Success: exit code: 0
#  Failure: Failed commands with the Error code in output and Non Zero Exit

import datetime
from subprocess import PIPE, Popen

# Variables
START = datetime.datetime.now()
CLUSTER_NAME = "ceph"
POOL_NAME = "test_rbd_pool"
F_COUNT = 0

# List
failed_commands = []


# Exception Class
class CmdError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


# Function Executing the command
def cmd(args):
    global F_COUNT
    while " " in args:
        args.remove(" ")
    print "************************************************************************************************************"
    command = " ".join(map(str, args))
    print "Executing the command :", command

    try:
        process = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        print "-----Output-----"
        print stdout, stderr
        if process.returncode == 0:
            return 0
        else:
            F_COUNT += 1
            print "Command Failed"
            raise CmdError(process.returncode)
    except CmdError as e:
        failed_commands.append(
            ["Command : " + command, ", Error Code : " + str(e.value)]
        )


if __name__ == "__main__":
    cmd(
        [
            "ceph",
            "osd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "pool",
            "delete",
            "{}".format(POOL_NAME),
            "{}".format(POOL_NAME),
            "--yes-i-really-really-mean-it",
        ]
    )

    cmd(
        [
            "ceph",
            "osd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "pool",
            "create",
            "{}".format(POOL_NAME),
            "128",
            "128",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "create",
            "-s",
            "1G",
            "{}".format(POOL_NAME) + "/" + "testimg",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "snap",
            "create",
            "{}".format(POOL_NAME) + "/" + "testimg" + "@" + "snapimg",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "snap",
            "protect",
            "{}".format(POOL_NAME) + "/" + "testimg" + "@" + "snapimg",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "clone",
            "{}".format(POOL_NAME) + "/" + "testimg" + "@" + "snapimg",
            "{}".format(POOL_NAME) + "/" + "clonetestimg",
        ]
    )

    for _ in range(1, 4):
        cmd(
            [
                "rbd",
                "--cluster",
                "{}".format(CLUSTER_NAME),
                "snap",
                "create",
                "{}".format(POOL_NAME)
                + "/"
                + "clonetestimg"
                + "@"
                + "clonesnap"
                + str(_),
            ]
        )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "flatten",
            "{}".format(POOL_NAME) + "/" + "clonetestimg",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "snap",
            "unprotect",
            "{}".format(POOL_NAME) + "/" + "testimg" + "@" + "snapimg",
        ]
    )

    cmd(
        [
            "rbd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "snap",
            "rm",
            "{}".format(POOL_NAME) + "/" + "testimg" + "@" + "snapimg",
        ]
    )

    cmd(
        [
            "ceph",
            "osd",
            "--cluster",
            "{}".format(CLUSTER_NAME),
            "pool",
            "delete",
            "{}".format(POOL_NAME),
            "{}".format(POOL_NAME),
            "--yes-i-really-really-mean-it",
        ]
    )

    print "Execution time for the script : " + str(datetime.datetime.now() - START)

    if F_COUNT == 0:
        print "********** TEST PASSED **********"
        exit(0)
    else:
        print "********** TEST FAILED **********"
        print "Total Failed Commands: ", F_COUNT
        print "FAILED COMMANDS:"
        for values in failed_commands:
            print values[0], values[1]
        exit(1)
