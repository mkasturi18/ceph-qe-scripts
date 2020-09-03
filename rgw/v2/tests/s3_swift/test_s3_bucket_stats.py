import os, sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
from v2.lib.resource_op import Config
import v2.utils.log as log
import v2.utils.utils as utils
import traceback
import argparse
import yaml
import json
from v2.lib.exceptions import TestExecError, RGWBaseException
from v2.utils.test_desc import AddTestInfo
from v2.lib.s3.write_io_info import IOInfoInitialize, BasicIOInfoStructure
from v2.lib.s3.write_io_info import AddUserInfo, BucketIoInfo
from v2.utils.utils import HttpResponseParser, RGWService
from v2.lib.read_io_info import ReadIOInfo
from v2.lib.s3.auth import Auth
from v2.tests.s3_swift import resuables
import v2.lib.resource_op as s3lib
import v2.lib.manage_data as manage_data

TEST_DATA_PATH = None


# create tenanted and non tenanted user
# create buckets for both users
# check the bucket stats

def test_exec(config):

    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()

    write_bucket_io_info = BucketIoInfo()
    io_info_initialize.initialize(basic_io_structure.initial())
    #ceph_conf = CephConfOp()
    #rgw_service = RGWService()

    # create user
    #all_users_info = s3lib.create_users(config.user_count)
    log.info('Create a user')
    user_info = s3lib.create_users(config.user_count)
    user_info = user_info[0]
    auth = Auth(user_info, ssl=config.ssl)
    rgw_conn = auth.do_auth()

    log.info('no of buckets to create: %s' % config.bucket_count)
    for bc in range(config.bucket_count):
        bucket_name= utils.gen_bucket_name_from_userid(user_info['user_id'], rand_no=1)
        log.info('creating bucket with name: %s' % bucket_name)
        bucket = resuables.create_bucket(bucket_name, rgw_conn, user_info)
        
        # create objects
        log.info('s3 objects to create: %s' % config.objects_count)
        for oc, size in list(config.mapped_sizes.items()):
            config.obj_size = size
            s3_object_name = utils.gen_s3_object_name(bucket_name, oc)
            log.info('s3 object name: %s' % s3_object_name)
            s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
            log.info('s3 object path: %s' % s3_object_path)
            resuables.upload_object(s3_object_name, bucket, TEST_DATA_PATH, config, user_info)
            # delete local object file
            utils.exec_shell_cmd('rm -rf %s' % s3_object_path)
    

            bucket_stats = utils.exec_shell_cmd("radosgw-admin bucket stats --bucket=%s" % bucket_name)
            bucket_stats_json = json.loads(bucket_stats)
    
            # verfiy the objects created
            objcts = bucket_stats_json['usage']['rgw.main']['num_objects']
            if objcts == config.objects_count:
                log.info('The object count verified')
            else:
                log.info('There is mismatch in object count')
    
    
            # verify the num_shards. The default bucket-index shards increased to 11. Ref: https://bugzilla.redhat.com/show_bug.cgi?id=1813349
            bkt_num_shards = bucket_stats_json['num_shards']
            default_num_shards = 11 
            if bkt_num_shards == default_num_shards:
                log.info('Verified the default num_shards[11]')
            else:
                raise TestExecError('The num_shards is not equal to 11. Ceph Version could be prior to RHCS4.1')
    
            # verify the bucket_id and marker remains same across reshards. Dynamic resharding
            #todo
            
            #List tthe objects
            log.info('listing all objects in bucket: %s' % bucket.name)
            objects = s3lib.resource_op({'obj': bucket, 'resource': 'objects', 'args': None})
            log.info('objects :%s' % objects)
            all_objects = s3lib.resource_op({'obj': objects, 'resource': 'all', 'args': None})
            log.info('all objects: %s' % all_objects)
            for obj in all_objects:
                log.info('object_name: %s' % obj.key)

            #Delete the objects
            log.info('deleting all objects in bucket')
            objects_deleted = s3lib.resource_op({'obj': objects, 'resource': 'delete', 'args': None})
            log.info('objects_deleted: %s' % objects_deleted)
            if objects_deleted is False:
                raise TestExecError('Resource execution failed: Object deletion failed')
            if objects_deleted is not None:
                response = HttpResponseParser(objects_deleted[0])
                if response.status_code == 200:
                    log.info('objects deleted ')
                else:
                    raise TestExecError("objects deletion failed")
            else:
                raise TestExecError("objects deletion failed")

            # verify 'rgw.none' does not appear in stats output. Ref: https://bugzilla.redhat.com/show_bug.cgi?id=1846035
            if 'rgw.none' not in bucket_stats_json['usage']:
                log.info('There is no rgw.none entry in the stats output')
            else:
                raise TestExecError('rgw.none entry exists in bucket stats')

    # remove the user created using purge-data
    log.info('completed!')
    resuables.remove_user(user_info)
    

    

if __name__ == '__main__':

    test_info = AddTestInfo('Verify bucket stats')
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = 'test_data'
        TEST_DATA_PATH = (os.path.join(project_dir, test_data_dir))
        log.info('TEST_DATA_PATH: %s' % TEST_DATA_PATH)
        if not os.path.exists(TEST_DATA_PATH):
            log.info('test data dir not exists, creating.. ')
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description='RGW S3 Automation')
        parser.add_argument('-c', dest="config",
                            help='RGW Test yaml configuration')
        args = parser.parse_args()
        yaml_file = args.config
        config = Config(yaml_file)
        config.read()
        if config.mapped_sizes is None:
            config.mapped_sizes = utils.make_mapped_sizes(config)

        test_exec(config)
        test_info.success_status('test passed')
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.info(e)
        log.info(traceback.format_exc())
        test_info.failed_status('test failed')
        sys.exit(1)




        
