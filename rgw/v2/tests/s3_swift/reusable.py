import glob
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import logging
import time
import timeit
import random

import v2.lib.manage_data as manage_data
import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.exceptions import MFAVersionError, TestExecError, EventRecordDataError
from v2.lib.rgw_config_opts import ConfigOpts
from v2.lib.s3.write_io_info import (
    BasicIOInfoStructure,
    BucketIoInfo,
    IOInfoInitialize,
    KeyIoInfo,
)
from v2.lib.sync_status import sync_status
from v2.utils.utils import HttpResponseParser, RGWService
from urllib import parse as urlparse

io_info_initialize = IOInfoInitialize()
basic_io_structure = BasicIOInfoStructure()
write_bucket_io_info = BucketIoInfo()
write_key_io_info = KeyIoInfo()
rgw_service = RGWService()

log = logging.getLogger()


def create_bucket(bucket_name, rgw, user_info):
    log.info("creating bucket with name: %s" % bucket_name)
    # bucket = s3_ops.resource_op(rgw_conn, 'Bucket', bucket_name_to_create)
    bucket = s3lib.resource_op(
        {"obj": rgw, "resource": "Bucket", "args": [bucket_name]}
    )
    created = s3lib.resource_op(
        {
            "obj": bucket,
            "resource": "create",
            "args": None,
            "extra_info": {"access_key": user_info["access_key"]},
        }
    )
    if created is False:
        raise TestExecError("Resource execution failed: bucket creation failed")
    if created is not None:
        response = HttpResponseParser(created)
        if response.status_code == 200:
            log.info("bucket created")
        else:
            raise TestExecError("bucket creation failed")
    else:
        raise TestExecError("bucket creation failed")
    return bucket


def upload_object(
    s3_object_name,
    bucket,
    TEST_DATA_PATH,
    config,
    user_info,
    append_data=False,
    append_msg=None,
):
    log.info("s3 object name: %s" % s3_object_name)
    s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
    log.info("s3 object path: %s" % s3_object_path)
    s3_object_size = config.obj_size
    if append_data is True:
        data_info = manage_data.io_generator(
            s3_object_path,
            s3_object_size,
            op="append",
            **{"message": "\n%s" % append_msg},
        )
    else:
        data_info = manage_data.io_generator(s3_object_path, s3_object_size)
    if data_info is False:
        TestExecError("data creation failed")
    log.info("uploading s3 object: %s" % s3_object_path)
    upload_info = dict({"access_key": user_info["access_key"]}, **data_info)
    s3_obj = s3lib.resource_op(
        {
            "obj": bucket,
            "resource": "Object",
            "args": [s3_object_name],
        }
    )
    object_uploaded_status = s3lib.resource_op(
        {
            "obj": s3_obj,
            "resource": "upload_file",
            "args": [s3_object_path],
            "extra_info": upload_info,
        }
    )
    if object_uploaded_status is False:
        raise TestExecError("Resource execution failed: object upload failed")
    if object_uploaded_status is None:
        log.info("object uploaded")


def upload_version_object(
    config, user_info, rgw_conn, s3_object_name, object_size, bucket, TEST_DATA_PATH
):
    # versioning upload
    log.info("versioning count: %s" % config.version_count)
    s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
    original_data_info = manage_data.io_generator(s3_object_path, object_size)
    if original_data_info is False:
        TestExecError("data creation failed")
    created_versions_count = 0
    for vc in range(config.version_count):
        log.info("version count for %s is %s" % (s3_object_name, str(vc)))
        log.info("modifying data: %s" % s3_object_name)
        modified_data_info = manage_data.io_generator(
            s3_object_path,
            object_size,
            op="append",
            **{"message": "\nhello for version: %s\n" % str(vc)},
        )
        if modified_data_info is False:
            TestExecError("data modification failed")
        log.info("uploading s3 object: %s" % s3_object_path)
        upload_info = dict(
            {
                "access_key": user_info["access_key"],
                "versioning_status": "enabled",
                "version_count_no": vc,
            },
            **modified_data_info,
        )
        s3_obj = s3lib.resource_op(
            {
                "obj": bucket,
                "resource": "Object",
                "args": [s3_object_name],
                "extra_info": upload_info,
            }
        )
        object_uploaded_status = s3lib.resource_op(
            {
                "obj": s3_obj,
                "resource": "upload_file",
                "args": [modified_data_info["name"]],
                "extra_info": upload_info,
            }
        )
        if object_uploaded_status is False:
            raise TestExecError("Resource execution failed: object upload failed")
        if object_uploaded_status is None:
            log.info("object uploaded")
            s3_obj = rgw_conn.Object(bucket.name, s3_object_name)
            log.info("current_version_id: %s" % s3_obj.version_id)
            key_version_info = basic_io_structure.version_info(
                **{
                    "version_id": s3_obj.version_id,
                    "md5_local": upload_info["md5"],
                    "count_no": vc,
                    "size": upload_info["size"],
                }
            )
            log.info("key_version_info: %s" % key_version_info)
            write_key_io_info.add_versioning_info(
                user_info["access_key"], bucket.name, s3_object_path, key_version_info
            )
            created_versions_count += 1
            log.info("created_versions_count: %s" % created_versions_count)
            log.info("adding metadata")
            metadata1 = {"m_data1": "this is the meta1 for this obj"}
            s3_obj.metadata.update(metadata1)
            metadata2 = {"m_data2": "this is the meta2 for this obj"}
            s3_obj.metadata.update(metadata2)
            log.info("metadata for this object: %s" % s3_obj.metadata)
            log.info("metadata count for object: %s" % (len(s3_obj.metadata)))
            if not s3_obj.metadata:
                raise TestExecError("metadata not created even adding metadata")
            versions = bucket.object_versions.filter(Prefix=s3_object_name)
            created_versions_count_from_s3 = len([v.version_id for v in versions])
            log.info(
                "created versions count on s3: %s" % created_versions_count_from_s3
            )
            if created_versions_count is created_versions_count_from_s3:
                log.info("no new versions are created when added metadata")
            else:
                raise TestExecError(
                    "version count mismatch, "
                    "possible creation of version on adding metadata"
                )
        s3_object_download_path = os.path.join(
            TEST_DATA_PATH, s3_object_name + ".download"
        )
        object_downloaded_status = s3lib.resource_op(
            {
                "obj": bucket,
                "resource": "download_file",
                "args": [s3_object_name, s3_object_download_path],
            }
        )
        if object_downloaded_status is False:
            raise TestExecError("Resource execution failed: object download failed")
        if object_downloaded_status is None:
            log.info("object downloaded")
        # checking md5 of the downloaded file
        s3_object_downloaded_md5 = utils.get_md5(s3_object_download_path)
        log.info("downloaded_md5: %s" % s3_object_downloaded_md5)
        log.info("uploaded_md5: %s" % modified_data_info["md5"])
        log.info("deleting downloaded version file")
        utils.exec_shell_cmd("sudo rm -rf %s" % s3_object_download_path)
    log.info("all versions for the object: %s\n" % s3_object_name)


def download_object(s3_object_name, bucket, TEST_DATA_PATH, s3_object_path, config):
    log.info("s3 object name to download: %s" % s3_object_name)
    s3_object_download_name = s3_object_name + "." + "download"
    s3_object_download_path = os.path.join(TEST_DATA_PATH, s3_object_download_name)
    object_downloaded_status = s3lib.resource_op(
        {
            "obj": bucket,
            "resource": "download_file",
            "args": [s3_object_name, s3_object_download_path],
        }
    )
    if object_downloaded_status is False:
        raise TestExecError("Resource execution failed: object download failed")
    if object_downloaded_status is None:
        log.info("object downloaded")

    s3_object_downloaded_md5 = utils.get_md5(s3_object_download_path)
    s3_object_uploaded_md5 = utils.get_md5(s3_object_path)
    log.info("s3_object_downloaded_md5: %s" % s3_object_downloaded_md5)
    log.info("s3_object_uploaded_md5: %s" % s3_object_uploaded_md5)
    if str(s3_object_uploaded_md5) == str(s3_object_downloaded_md5):
        log.info("md5 match")
        utils.exec_shell_cmd("rm -rf %s" % s3_object_download_path)
    else:
        raise TestExecError("md5 mismatch")
    if config.local_file_delete is True:
        log.info("deleting local file created after the upload")
        utils.exec_shell_cmd("rm -rf %s" % s3_object_path)


def upload_object_with_tagging(
    s3_object_name,
    bucket,
    TEST_DATA_PATH,
    config,
    user_info,
    obj_tag,
    append_data=False,
    append_msg=None,
):
    log.info("s3 object name: %s" % s3_object_name)
    s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
    log.info("s3 object path: %s" % s3_object_path)
    s3_object_size = config.obj_size
    if append_data is True:
        data_info = manage_data.io_generator(
            s3_object_path,
            s3_object_size,
            op="append",
            **{"message": "\n%s" % append_msg},
        )
    else:
        data_info = manage_data.io_generator(s3_object_path, s3_object_size)
    if data_info is False:
        TestExecError("data creation failed")
    log.info("uploading s3 object with object tagging enabled: %s" % s3_object_path)
    bucket.put_object(Key=s3_object_name, Body=s3_object_path, Tagging=obj_tag)


def upload_mutipart_object(
    s3_object_name,
    bucket,
    TEST_DATA_PATH,
    config,
    user_info,
    append_data=False,
    append_msg=None,
):
    log.info("s3 object name: %s" % s3_object_name)
    s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
    log.info("s3 object path: %s" % s3_object_path)
    s3_object_size = config.obj_size
    split_size = config.split_size if hasattr(config, "split_size") else 5
    log.info("split size: %s" % split_size)
    if append_data is True:
        data_info = manage_data.io_generator(
            s3_object_path,
            s3_object_size,
            op="append",
            **{"message": "\n%s" % append_msg},
        )
    else:
        data_info = manage_data.io_generator(s3_object_path, s3_object_size)
    if data_info is False:
        TestExecError("data creation failed")
    mp_dir = os.path.join(TEST_DATA_PATH, s3_object_name + ".mp.parts")
    log.info("mp part dir: %s" % mp_dir)
    log.info("making multipart object part dir")
    mkdir = utils.exec_shell_cmd("sudo mkdir %s" % mp_dir)
    if mkdir is False:
        raise TestExecError("mkdir failed creating mp_dir_name")
    utils.split_file(s3_object_path, split_size, mp_dir + "/")
    parts_list = sorted(glob.glob(mp_dir + "/" + "*"))
    log.info("parts_list: %s" % parts_list)
    log.info("uploading s3 object: %s" % s3_object_path)
    upload_info = dict(
        {"access_key": user_info["access_key"], "upload_type": "multipart"}, **data_info
    )
    s3_obj = s3lib.resource_op(
        {
            "obj": bucket,
            "resource": "Object",
            "args": [s3_object_name],
        }
    )
    log.info("initiating multipart upload")
    mpu = s3lib.resource_op(
        {
            "obj": s3_obj,
            "resource": "initiate_multipart_upload",
            "args": None,
            "extra_info": upload_info,
        }
    )
    part_number = 1
    parts_info = {"Parts": []}
    log.info("no of parts: %s" % len(parts_list))
    for each_part in parts_list:
        log.info("trying to upload part: %s" % each_part)
        part = mpu.Part(part_number)
        # part_upload_response = part.upload(Body=open(each_part))
        part_upload_response = s3lib.resource_op(
            {
                "obj": part,
                "resource": "upload",
                "kwargs": dict(Body=open(each_part, mode="rb")),
            }
        )
        if part_upload_response is not False:
            response = HttpResponseParser(part_upload_response)
            if response.status_code == 200:
                log.info("part uploaded")
                if config.local_file_delete is True:
                    log.info("deleting local file part")
                    utils.exec_shell_cmd("sudo rm -rf %s" % each_part)
            else:
                raise TestExecError("part uploading failed")
        part_info = {"PartNumber": part_number, "ETag": part_upload_response["ETag"]}
        parts_info["Parts"].append(part_info)
        if each_part != parts_list[-1]:
            # increase the part number only if the current part is not the last part
            part_number += 1
        log.info("curr part_number: %s" % part_number)
    # log.info('parts_info so far: %s'% parts_info)
    if len(parts_list) == part_number:
        log.info("all parts upload completed")
        mpu.complete(MultipartUpload=parts_info)
        log.info("multipart upload complete for key: %s" % s3_object_name)


def enable_versioning(bucket, rgw_conn, user_info, write_bucket_io_info):
    log.info("bucket versioning test on bucket: %s" % bucket.name)
    # bucket_versioning = s3_ops.resource_op(rgw_conn, 'BucketVersioning', bucket.name)
    bucket_versioning = s3lib.resource_op(
        {"obj": rgw_conn, "resource": "BucketVersioning", "args": [bucket.name]}
    )
    # checking the versioning status
    # version_status = s3_ops.resource_op(bucket_versioning, 'status')
    version_status = s3lib.resource_op(
        {"obj": bucket_versioning, "resource": "status", "args": None}
    )
    if version_status is None:
        log.info("bucket versioning still not enabled")
    # enabling bucket versioning
    # version_enable_status = s3_ops.resource_op(bucket_versioning, 'enable')
    version_enable_status = s3lib.resource_op(
        {"obj": bucket_versioning, "resource": "enable", "args": None}
    )
    response = HttpResponseParser(version_enable_status)
    if response.status_code == 200:
        log.info("version enabled")
        write_bucket_io_info.add_versioning_status(
            user_info["access_key"], bucket.name, "enabled"
        )
    else:
        raise TestExecError("version enable failed")


def generate_totp(seed):
    cmd = "oathtool -d6 --totp %s" % seed
    totp_token = utils.exec_shell_cmd(cmd)
    return totp_token.rstrip("\n")


def enable_mfa_versioning(
    bucket, rgw_conn, SEED, serial, user_info, write_bucket_io_info
):
    log.info("bucket MFA and versioning test on bucket: %s" % bucket.name)
    bucket_versioning = s3lib.resource_op(
        {"obj": rgw_conn, "resource": "BucketVersioning", "args": [bucket.name]}
    )
    # checking the versioning status
    version_status = s3lib.resource_op(
        {"obj": bucket_versioning, "resource": "status", "args": None}
    )
    if version_status is None:
        log.info("bucket mfa and versioning still not enabled")

    # generate MFA token to authenticate
    token = generate_totp(SEED)
    mfa_token = serial + " " + token

    # put mfa and bucket versioning
    mfa_version_put = s3lib.resource_op(
        {
            "obj": bucket_versioning,
            "resource": "put",
            "kwargs": dict(
                MFA=(mfa_token),
                VersioningConfiguration={"MFADelete": "Enabled", "Status": "Enabled"},
                ExpectedBucketOwner=user_info["user_id"],
            ),
        }
    )

    if mfa_version_put is False:
        return token, mfa_version_put

    response = HttpResponseParser(mfa_version_put)
    if response.status_code == 200:
        log.info("MFA and version enabled")
    else:
        raise MFAVersionError("bucket mfa and versioning enable failed")


def put_get_bucket_lifecycle_test(bucket, rgw_conn, rgw_conn2, life_cycle_rule, config):
    bucket_life_cycle = s3lib.resource_op(
        {
            "obj": rgw_conn,
            "resource": "BucketLifecycleConfiguration",
            "args": [bucket.name],
        }
    )
    put_bucket_life_cycle = s3lib.resource_op(
        {
            "obj": bucket_life_cycle,
            "resource": "put",
            "kwargs": dict(LifecycleConfiguration=life_cycle_rule),
        }
    )
    log.info("put bucket life cycle:\n%s" % put_bucket_life_cycle)
    if put_bucket_life_cycle is False:
        raise TestExecError("Resource execution failed: put bucket lifecycle failed")
    if put_bucket_life_cycle is not None:
        response = HttpResponseParser(put_bucket_life_cycle)
        if response.status_code == 200:
            log.info("bucket life cycle added")
        else:
            raise TestExecError("bucket lifecycle addition failed")
    log.info("trying to retrieve bucket lifecycle config")
    get_bucket_life_cycle_config = s3lib.resource_op(
        {
            "obj": rgw_conn2,
            "resource": "get_bucket_lifecycle_configuration",
            "kwargs": dict(Bucket=bucket.name),
        }
    )
    if get_bucket_life_cycle_config is False:
        raise TestExecError("bucket lifecycle config retrieval failed")
    if get_bucket_life_cycle_config is not None:
        response = HttpResponseParser(get_bucket_life_cycle_config)
        if response.status_code == 200:
            log.info("bucket life cycle retrieved")
        else:
            raise TestExecError("bucket lifecycle config retrieval failed")
    else:
        raise TestExecError("bucket life cycle retrieved")
    objs_total = (config.test_ops["version_count"]) * (config.objects_count)
    for rule in config.lifecycle_conf:
        if rule.get("Expiration", {}).get("Date", False):
            # todo: need to get the interval value from yaml file
            log.info("wait for 60 seconds")
            time.sleep(60)
        else:
            for time_interval in range(19):
                bucket_stats_op = utils.exec_shell_cmd(
                    "radosgw-admin bucket stats --bucket=%s" % bucket.name
                )
                json_doc1 = json.loads(bucket_stats_op)
                obj_pre_lc = json_doc1["usage"]["rgw.main"]["num_objects"]
                if obj_pre_lc == objs_total:
                    time.sleep(30)
                else:
                    raise TestExecError("Objects expired before the expected days")
            time.sleep(60)

    log.info("testing if lc is applied via the radosgw-admin cli")
    op = utils.exec_shell_cmd("radosgw-admin lc list")
    json_doc = json.loads(op)
    for i, entry in enumerate(json_doc):
        print(i)
        print(entry["status"])
        if entry["status"] == "COMPLETE" or entry["status"] == "PROCESSING":
            log.info("LC is applied on the bucket")
        else:
            raise TestExecError("LC is not applied")


def remove_user(user_info, cluster_name="ceph", tenant=False):
    log.info("Removing user")
    if tenant:
        cmd = "radosgw-admin user rm --purge-keys --purge-data --uid=%s --tenant=%s" % (
            user_info["user_id"],
            tenant,
        )
    else:
        cmd = "radosgw-admin user rm --purge-keys --purge-data --uid=%s" % (
            user_info["user_id"]
        )
    out = utils.exec_shell_cmd(cmd)
    return out


def rename_user(old_username, new_username, tenant=False):
    """"""
    if tenant:
        cmd = "radosgw-admin user rename --uid=%s --new-uid=%s --tenant=%s" % (
            old_username,
            new_username,
            tenant,
        )
    else:
        cmd = "radosgw-admin user rename --uid=%s --new-uid=%s" % (
            old_username,
            new_username,
        )
    out = utils.exec_shell_cmd(cmd)
    log.info("Renamed user %s to %s" % (old_username, new_username))
    return out


def rename_bucket(old_bucket, new_bucket, userid, tenant=False):
    """"""
    validate = "radosgw-admin bucket list"
    if tenant:
        cmd = "radosgw-admin bucket link --bucket=%s --bucket-new-name=%s --uid=%s --tenant=%s" % (
            str(tenant) + "/" + old_bucket,
            str(tenant) + "/" + new_bucket,
            userid,
            tenant,
        )
    else:
        cmd = "radosgw-admin bucket link --bucket=%s --bucket-new-name=%s --uid=%s" % (
            "/" + old_bucket,
            new_bucket,
            userid,
        )
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError("RGW Bucket rename error")
    response = utils.exec_shell_cmd(validate)
    if old_bucket in json.loads(response):
        raise TestExecError("RGW Bucket rename validation error")
    log.info("Renamed bucket %s to %s" % (old_bucket, new_bucket))
    return out


def unlink_bucket(curr_uid, bucket, tenant=False):
    """"""
    if tenant:
        cmd = "radosgw-admin bucket unlink --bucket=%s --uid=%s --tenant=%s" % (
            bucket,
            curr_uid,
            tenant,
        )
    else:
        cmd = "radosgw-admin bucket unlink --bucket=%s --uid=%s" % (bucket, curr_uid)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError("RGW Bucket unlink error")
    return out


def link_chown_to_tenanted(new_uid, bucket, tenant):
    """"""
    cmd = "radosgw-admin bucket link --bucket=%s --uid=%s --tenant=%s" % (
        "/" + bucket,
        new_uid,
        tenant,
    )
    out1 = utils.exec_shell_cmd(cmd)
    if out1 is False:
        raise TestExecError("RGW Bucket link error")
    log.info("output :%s" % out1)
    cmd1 = "radosgw-admin bucket chown --bucket=%s --uid=%s --tenant=%s" % (
        bucket,
        new_uid,
        tenant,
    )
    out2 = utils.exec_shell_cmd(cmd1)
    if out2 is False:
        raise TestExecError("RGW Bucket chown error")
    log.info("output :%s" % out2)
    return


def link_chown_to_nontenanted(new_uid, bucket, tenant):
    """"""
    cmd2 = "radosgw-admin bucket link --bucket=%s --uid=%s" % (
        tenant + "/" + bucket,
        new_uid,
    )
    out3 = utils.exec_shell_cmd(cmd2)
    if out3 is False:
        raise TestExecError("RGW Bucket link error")
    log.info("output :%s" % out3)
    cmd3 = "radosgw-admin bucket chown --bucket=%s --uid=%s" % (bucket, new_uid)
    out4 = utils.exec_shell_cmd(cmd3)
    if out4 is False:
        raise TestExecError("RGW Bucket chown error")
    log.info("output :%s" % out4)
    return


def delete_objects(bucket):
    """
    deletes the objects in a given bucket
    :param bucket: S3Bucket object
    """
    log.info("listing all objects in bucket: %s" % bucket.name)
    objects = s3lib.resource_op({"obj": bucket, "resource": "objects", "args": None})
    log.info("objects :%s" % objects)
    all_objects = s3lib.resource_op({"obj": objects, "resource": "all", "args": None})
    log.info("all objects: %s" % all_objects)
    for obj in all_objects:
        log.info("object_name: %s" % obj.key)
    log.info("deleting all objects in bucket")
    objects_deleted = s3lib.resource_op(
        {"obj": objects, "resource": "delete", "args": None}
    )
    log.info("objects_deleted: %s" % objects_deleted)
    if objects_deleted is False:
        raise TestExecError("Resource execution failed: Object deletion failed")
    if objects_deleted is not None:
        response = HttpResponseParser(objects_deleted[0])
        if response.status_code == 200:
            log.info("objects deleted ")
        else:
            raise TestExecError("objects deletion failed")
    else:
        raise TestExecError("objects deletion failed")


def list_objects(bucket):
    """
    list the objects in a given bucket
    :param bucket: S3Bucket object
    """
    log.info("listing all objects in bucket: %s" % bucket.name)
    objects = s3lib.resource_op({"obj": bucket, "resource": "objects", "args": None})
    log.info("objects :%s" % objects)
    all_objects = s3lib.resource_op({"obj": objects, "resource": "all", "args": None})
    log.info("all objects: %s" % all_objects)
    for obj in all_objects:
        log.info("object_name: %s" % obj.key)


def list_versioned_objects(bucket, s3_object_name, s3_object_path, rgw_conn):
    """
    list all versions of the objects in a given bucket
    :param bucket: S3Bucket object
    """
    versions = bucket.object_versions.filter(Prefix=s3_object_name)
    log.info(f"listing all the versions of objects {s3_object_name}")
    for version in versions:
        log.info(f"key_name: {version.object_key} --> version_id: {version.version_id}")


def delete_version_object(
    bucket,
    s3_object_name,
    s3_object_path,
    rgw_conn,
    user_info,
):
    """
    deletes single object and its versions
    :param bucket: S3bucket object
    :param s3_object_name: s3 object name
    :param s3_object_path: path of the object created in the client
    :param rgw_conn: rgw connection
    :param user_info: user info dict containing access_key, secret_key and user_id
    """
    versions = bucket.object_versions.filter(Prefix=s3_object_name)
    log.info("deleting s3_obj keys and its versions")
    s3_obj = s3lib.resource_op(
        {"obj": rgw_conn, "resource": "Object", "args": [bucket.name, s3_object_name]}
    )
    log.info("deleting versions for s3 obj: %s" % s3_object_name)
    for version in versions:
        log.info("trying to delete obj version: %s" % version.version_id)
        del_obj_version = s3lib.resource_op(
            {
                "obj": s3_obj,
                "resource": "delete",
                "kwargs": dict(VersionId=version.version_id),
            }
        )
        log.info("response:\n%s" % del_obj_version)
        if del_obj_version is not None:
            response = HttpResponseParser(del_obj_version)
            if response.status_code == 204:
                log.info("version deleted ")
                write_key_io_info.delete_version_info(
                    user_info["access_key"],
                    bucket.name,
                    s3_object_path,
                    version.version_id,
                )
            else:
                raise TestExecError("version  deletion failed")
        else:
            raise TestExecError("version deletion failed")
    log.info("available versions for the object")
    versions = bucket.object_versions.filter(Prefix=s3_object_name)
    for version in versions:
        log.info(
            "key_name: %s --> version_id: %s" % (version.object_key, version.version_id)
        )


def delete_bucket(bucket):
    """
    deletes a given bucket
    :param bucket: s3Bucket object
    """
    log.info("listing objects if any")
    objs = bucket.objects.all()
    count = sum(1 for _ in bucket.objects.all())
    if count > 0:
        log.info(f"objects not deleted, count is:{count}")
        for ob in objs:
            log.info(f"object: {ob.key}")

    log.info("deleting bucket: %s" % bucket.name)
    bucket_deleted_response = s3lib.resource_op(
        {"obj": bucket, "resource": "delete", "args": None}
    )
    log.info("bucket_deleted_status: %s" % bucket_deleted_response)
    if bucket_deleted_response is not None and isinstance(
        bucket_deleted_response, dict
    ):
        response = HttpResponseParser(bucket_deleted_response)
        if response.status_code == 204:
            log.info("bucket deleted ")
        else:
            raise TestExecError("bucket deletion failed")
    else:
        raise TestExecError("bucket deletion failed")


def set_gc_conf(ceph_conf, conf):
    log.info("making changes to ceph.conf")
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.bluestore_block_size,
        str(conf.get("bluestore_block_size", 1549267441664)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_max_queue_size,
        str(conf.get("rgw_gc_max_queue_size", 367788)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_processor_max_time,
        str(conf.get("rgw_gc_processor_max_time", 3600)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_max_concurrent_io,
        str(conf.get("rgw_gc_max_concurrent_io", 10)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_objexp_gc_interval,
        str(conf.get("rgw_objexp_gc_interval", 10)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_max_trim_chunk,
        str(conf.get("rgw_gc_max_trim_chunk", 32)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_obj_min_wait,
        str(conf.get("rgw_gc_obj_min_wait", 10)),
    )
    ceph_conf.set_to_ceph_conf(
        "global",
        ConfigOpts.rgw_gc_processor_period,
        str(conf.get("rgw_gc_processor_period", 10)),
    )
    log.info("trying to restart services")
    srv_restarted = rgw_service.restart()
    time.sleep(30)
    if srv_restarted is False:
        raise TestExecError("RGW service restart failed")
    else:
        log.info("RGW service restarted")
    # Delete gc queue
    pool_name = utils.exec_shell_cmd("ceph df |awk '{ print $1 }'| grep rgw.log")
    pool_name = pool_name.replace("\n", "")
    for i in range(0, 32):
        utils.exec_shell_cmd("rados rm gc.%d -p %s -N gc" % (i, pool_name))


def verify_gc():
    op = utils.exec_shell_cmd("radosgw-admin gc list")
    # op variable will capture command output such as entire gc list or error like ERROR: failed to list objs: (22) Invalid argument
    final_op = op.find("ERROR") or op.find("Invalid argument")
    return final_op


def check_for_crash():
    """
    check for crash on cluster
    """
    ceph_version_id, ceph_version_name = utils.get_ceph_version()
    if ceph_version_name == "nautilus":
        log.info("check for any new crashes on the ceph cluster ")
        ceph_crash = utils.exec_shell_cmd("ceph crash ls-new")
        if ceph_crash:
            ceph_crash_all = ceph_crash.split()
            no_of_crashes = len(ceph_crash_all)
            for i in range(3, no_of_crashes):
                if i % 3 == 0:
                    ceph_crash_id, ceph_crash_entity = (
                        ceph_crash_all[i],
                        ceph_crash_all[i + 1],
                    )
                    log.info(f"ceph daemon {ceph_crash_entity} crashed!")
                    crash_info = utils.exec_shell_cmd(
                        "ceph crash info %s" % ceph_crash_id
                    )
            log.info(
                "archiving the crashes to silence health warnings! to view the crashes use the command: ceph crash ls"
            )
            utils.exec_shell_cmd("ceph crash archive-all")
        else:
            log.info("No ceph daemon crash found")
        return ceph_crash


def time_to_list_via_radosgw(bucket_name, listing):
    """
    Time taken to list via radosgw-admin command.
    :param bucket: s3Bucket object
    :param listing: ordered or unordered listing
    """
    if listing == "ordered":
        log.info("listing via radosgw-admin bucket list --max-entries=.. --bucket <>")
        cmd = "radosgw-admin bucket list --max-entries=100000 --bucket=%s " % (
            bucket_name
        )
        time_taken = timeit.timeit(utils.exec_shell_cmd(cmd), globals=globals())
        return time_taken

    if listing == "unordered":
        log.info(
            "listing via radosgw-admin bucket list --max-entries=.. --bucket <> --allow-unordered"
        )
        cmd = (
            "radosgw-admin bucket list --max-entries=100000 --bucket=%s --allow-unordered"
            % (bucket_name)
        )
        time_taken = timeit.timeit(utils.exec_shell_cmd(cmd), globals=globals())
        return time_taken


def time_to_list_via_boto(bucket_name, rgw):
    """
    Time taken to list via boto
    :param bucket: s3Bucket object
    """
    bucket = s3lib.resource_op(
        {"obj": rgw, "resource": "Bucket", "args": [bucket_name]}
    )

    log.info("listing all objects in bucket: %s" % bucket)
    objects = s3lib.resource_op({"obj": bucket, "resource": "objects", "args": None})
    time_taken = timeit.timeit(lambda: bucket.objects.all(), globals=globals())
    return time_taken


def check_sync_status(retry=None, delay=None):
    """
    Check sync status if its a multisite cluster
    """
    is_multisite = utils.is_cluster_multisite()
    if is_multisite:
        sync_status()

def start_kafka_broker_consumer(topic_name,event_record_path):
    """
    start zookeeper, kafka broker and listen to consumer
    topic_name: name of the topic to listen to
    event_record_path: path to store the event records
    """
    #check if the file at event_record_path already exists
    if os.path.isfile(event_record_path):
        log.info('stale event record file exists, deleting it before creating a new file.')
        cmd = f'rm -f {event_record_path}'
        os.system(cmd)

    #start zookeeper
    start_zookeepeer_kafka = os.system('/root/kafka/kafka*/bin/zookeeper-server-start.sh /root/kafka/kafka*/config/zookeeper.properties > /dev/null 2>&1 &')
    log.info('zookeeper started \n')

    #start kafka broker
    start_kafka_broker = os.system('/root/kafka/kafka*/bin/kafka-server-start.sh /root/kafka/kafka*/config/server.properties > /dev/null 2>&1 &')
    log.info ('kafka broker started \n')
    log.info('sleep for 10 seconds and start the consumer')
    time.sleep(10)

    #start kafka consumer 
    cmd = f'/root/kafka/kafka*/bin/kafka-console-consumer.sh --bootstrap-server kafka://localhost:9092 --from-beginning --topic {topic_name} --zookeeper localhost:2181 --timeout-ms 20000 >> {event_record_path}'
    start_consumer_kafka = os.system(cmd)


def stop_kafka_broker():
    """
    stop zookeeper and kafka broker
    """
    #stop kafka broker followed by zookeeper
    log.info('consumer started and sleep of 10 seconds before stopping the kafka broker and zookeeper')
    time.sleep(10)
    cmd = '/root/kafka/kafka*/bin/kafka-server-stop.sh && /root/kafka/kafka*/bin/zookeeper-server-stop.sh'
    stop_kafka_server = os.system(cmd)
    log.info('kafka server and zookeeper server stopped')

def create_topic(sns_client,endpoint,ack_type,topic_name,persistent_flag=False):
    """
    to create topic with specified endpoint , ack_level 
    return: topic ARN
    """
    endpoint_args = 'push-endpoint='+endpoint+'://localhost&verify-ssl=False&kafka-ack-level='+ack_type
    if persistent_flag:
        endpoint_args = endpoint_args+'&persistent=true' 
    attributes = {nvp[0] : nvp[1] for nvp in urlparse.parse_qsl(endpoint_args, keep_blank_values=True)}
    get_topic = sns_client.create_topic(Name=topic_name, Attributes=attributes)
    topic_arn = get_topic['TopicArn']
    
    log.info(f"topic_ARN is : {topic_arn}")     
    return topic_arn

def get_topic(client,topic_arn):
    """
    get the topic with spedified topic_arn
    """
    get_topic_info = client.get_topic_attributes(TopicArn=topic_arn)
    get_topic_info_json = json.dumps(get_topic_info,indent=2)
    if get_topic_info is False:
        raise TestExecError("topic creation failed")
    else:
        log.info(f"get topic attributes: {get_topic_info_json}")


def put_bucket_notification(rgw_s3_client,bucketname,notification_name,topic_arn,event):
    """
    put bucket notification on bucket for specified events with given endpoint and topic 
    """
    log.info(f'put bucket notification on {bucketname}')
    put_bkt_notification = rgw_s3_client.put_bucket_notification_configuration(
                        Bucket=bucketname,
                        NotificationConfiguration={
                            'TopicConfigurations': [
                            {        
                                'Id': notification_name,
                                'TopicArn': topic_arn,
                                'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
                            }]})
    if put_bkt_notification is False:
        raise TestExecError("put bucket notification failed")

def get_bucket_notification(rgw_s3_client,bucketname):
    """
    get bucket notification for a given bucket
    """
    get_bkt_notification = rgw_s3_client.get_bucket_notification_configuration(Bucket=bucketname)
    if get_bkt_notification is False:
        raise TestExecError(f"failed to get bucket notification for bucket : {bucketname}")
    get_bucket_notification_json = json.dumps(get_bkt_notification,indent=2)
    log.info(f"bucket notification for bucket: {bucketname} is {get_bucket_notification_json}")

def verify_event_record(event_type, bucket,event_record_path):
    """
    verify event records
    """
    if os.path.getsize(event_record_path) == 0: 
        raise EventRecordDataError('event record not generated! File is empty')
    
    #read the file event_record
    with open(event_record_path,"r") as records:
        for record in records:
            event_record = record.strip()
            log.info(f' event record \n {record}')
            event_record_json = json.loads(event_record)
            
            #verify "eventTime" attribute
            eventTime = event_record_json['Records'][0]['eventTime']
    
            # verify eventTime reflects correct timestamp. BZ:https://bugzilla.redhat.com/show_bug.cgi?id=1959254
            if '0.000000' in eventTime:
                raise EventRecordDataError('eventTime 0.000000 in event record')
            if 'T' in eventTime:
                log.info(f'eventTime: {eventTime},Timestamp format validated')
            else:
                raise EventRecordDataError('eventTime: Incorrect timestamp format')
    
            #verify "eventName" attribute
            eventName = event_record_json["Records"][0]["eventName"]
            #s3Prefix removed with BZ: https://bugzilla.redhat.com/show_bug.cgi?id=1966676
            ceph_version_id, ceph_version_name = utils.get_ceph_version()
            if 's3:' in eventName and ceph_version_name == 'Pacific':
                raise EventRecordDataError('eventName: s3 prefix in eventName')
    
            # verify event record for a particular event type
            if 'Delete' in event_type:
                events = ['Put','Delete']
            if 'Copy' in event_type:
                events = ['Put','Copy']
            if 'Multipart' in event_type:
                events = ['Post','Put','CompleteMultipartUpload']
            for event in events:
                if event in eventName:
                    log.info(f'eventName: {eventName} in event record')

            # fetch bucket details and verify bucket attributes in event record
            bucket_stats = utils.exec_shell_cmd(
                 "radosgw-admin bucket stats --bucket  %s" % bucket
                    )
            bucket_stats_json = json.loads(bucket_stats)
            log.info('verify bucket attributes in event record')
            #verify bucket name in event record
            bucket_name = event_record_json["Records"][0]["s3"]["bucket"]["name"]
            if bucket in bucket_name:
                log.info(f'Bucket-name: {bucket_name}')
            else:
                raise EventRecordDataError('BucketName not in event record')

            #verify bucket id in event record
            bucket_id = bucket_stats_json["id"]
            bucket_id_evnt = event_record_json["Records"][0]["s3"]["bucket"]["id"]
            if bucket_id == bucket_id_evnt:
                log.info(f'Bucket-id: {bucket_id}')
            else:
                raise EventRecordDataError('BucketID not in event record')
    
            #verify bucket owner in event record
            bucket_owner = bucket_stats_json["owner"]
            bkt_owner_evnt = event_record_json["Records"][0]["s3"]["bucket"]["ownerIdentity"]["principalId"]
            if bucket_owner == bkt_owner_evnt:
                log.info(f'Bucket-owner: {bucket_owner}')
            else:
                raise EventRecordDataError('BucketOwner not in event record')


            log.info('verify object attributes')
            # verify object size attribute
            size = event_record_json["Records"][0]["s3"]["object"]["size"]
            #verify object size is not 0, for the object. BZ:https://bugzilla.redhat.com/show_bug.cgi?id=1960648
            if size == 0:
                if 'Post' in eventName :
                    log.info('Expected behavior') 
                else:
                    raise EventRecordDataError('size: Object size is 0')
            else:
                log.info(f'size: {size}')

    # delete event record
    log.info("deleting local file to verify event record")
    utils.exec_shell_cmd("rm -rf %s" % event_record_path)

