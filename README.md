## cephbackup

Cloudcluster ceph rbd incremental backup tool.
* Incremental: incremental backups within a given backup window based on rbd snapshots
* Full: full image exports based on a temporary snapshot

## Building
$ git clone https://github.com/10779164/ceph-backup.git
$ cd ceph-backup
$ python setup.py install (python setup.py check)

## Prepare config file
you can create config file "cephbackup.conf" by ./cephbackup/cephbackup.conf.simple. like:

[rbd]
backup init = 7 
backup directory = /backup
images = *
ceph config = /etc/ceph/ceph.conf
backup mode = incremental

$ mkdir -p /etc/cephbackup/
$ cp cephbackup.conf /etc/cephbackup


## Running backup
$$ cephbackup

## Backup restore
### Go to the directory where you want to restore the image backup files(The directory define in your cephbackup config file)
#### 1.Running full backup restore
$$ rbd import {imagename}-{snapname}.full dest_image
eg: rbd import test-SNAPSHOT-20190530145240.full test1

#### 2.Recreate basic snapshot
$$ rbd snap create dest_image@{snapname} 
eg: rbd snap create test1@SNAPSHOT-20190530145240

#### 3.Restore incremental backup file
$$ rbd import-diff {imagename}-{snapname}.diff_from{...} dest_image
eg: rbd import test-SNAPSHOT-20190530153047.diff_from_SNAPSHOT-20190530145240 test1
    rbd import-diff test-SNAPSHOT-20190530153047.diff_from_SNAPSHOT-20190530145240 test1
    rbd import-diff test-SNAPSHOT-20190530154156.diff_from_SNAPSHOT-20190530153047 test1


