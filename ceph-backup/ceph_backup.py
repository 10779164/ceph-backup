#!encoding=utf8
#!/usr/bin/python	
from  executor import execute
import re
import time
import argparse
import os
import rados
import rbd

class cephbackup():
    '''
    rbd commands:
    rbd snap create rbd/image@snap_name
    rbd export-diff --from snap newest_snap rbd/image@cur_snap /path/filename
    '''
    PREFIX = 'SNAPSHOT'
    TIME_FMT = time.strftime('%Y%m%d%H%M%S')
    SNAPSHOT_NAME = '{}-{}'.format(PREFIX,TIME_FMT)

    def __init__(self, pool, images, backup_dest, ceph_conf, check_mode=False, compress_mode=False, window_size=7, window_unit='days'):
 	self._pool=pool
	self._images = images
        self._backup_dest = backup_dest
        self._check_mode = check_mode
        self._compress_mode = compress_mode
        self._window_size = window_size
        self._window_unit = window_unit
	self._ceph_conf= ceph_conf

 	cluster = rados.Rados(conffile=ceph_conf)
	cluster.connect()
 	self._ceph_ioctx = cluster.open_ioctx(pool)
	self._ceph_rbd = rbd.RBD()
	
	if len(self._images) == 1 and self._images[0] == '*':
	    self._images = self._get_images()
	else:
	    self._images = images.split(',')

    def _get_images(self):
	#return a list	
	return self._ceph_rbd.list(self._ceph_ioctx)

    '''
    imagename:
    for imagename in self._images:
	 pass
    '''

    def _get_snapshots(self,imagename):
	#return a dic
        #{'namespace': 0, 'id': 266L, 'name': u'SNAPSHOT-20190530004929', 'size': 1073741824L}
	# dic.get('name') return snapshot name
        prefix_length = len(cephbackup.PREFIX)
	image = rbd.Image(self._ceph_ioctx,imagename)
	snapshots = []
	for snapshot in image.list_snaps():
	    if snapshot.get('name')[0:prefix_length] == cephbackup.PREFIX:
		snapshots.append(snapshot.get('name'))
	return snapshots

    def _get_num_snapshosts(self, imagename):
        return len(self._get_snapshots(imagename)) 

    def _get_newest_snapshot(self,imagename):
	snapshots = self._get_snapshots(imagename)
	if len(snapshots) is 0:
	    return None
	return max(snapshots)

    def _get_oldest_snapshot(self,imagename):
	snapshots = self._get_snapshots(imagename)
	if len(snapshots) is 0:
            return None
        return min(snapshots)

    def _create_snapshot(self,imagename):
	image = rbd.Image(self._ceph_ioctx,imagename) 
	image.create_snap(cephbackup.SNAPSHOT_NAME)
	return cephbackup.SNAPSHOT_NAME

    def _delete_overage_snapshot(self,imagename,full_snapname):
	snapshots = self._get_snapshots(imagename)
	image = rbd.Image(self._ceph_ioctx,imagename)
	for overage_snapshot in snapshots:
	    if overage_snapshot != full_snapname: 
	        image.remove_snap(overage_snapshot)
	        print "Deleted snapshot {pool}/{snapname}".format(pool=self._pool,snapname=overage_snapshot)

    def _delete_overage_backupfile(self,imagename):
	#snapshots = self._get_snapshots(imagename)
	dest_dir = os.path.join(self._backup_dest, self._pool, imagename)
	for dest_file in os.listdir(dest_dir):
	    if re.match(r"{image}@(.*?)".format(image=imagename),dest_file):
		backup_file = dest_dir+'/'+dest_file
		print "Deleting backup file {backup_file}...".format(backup_file=dest_file)
		os.remove(backup_file)

    def _export_full_snapshot(self,imagename):
	backupname=imagename+'@'+cephbackup.SNAPSHOT_NAME+".full"
	dest_dir=os.path.join(self._backup_dest, self._pool, imagename)
    	if not os.path.exists(dest_dir):
	    os.makedirs(dest_dir)
	full_backupname=dest_dir+'/'+backupname	
	execute("rbd export {pool}/{image} {dest}".format(pool=self._pool,image=imagename,dest=full_backupname),sudo=True)
	print "Exporting image {pool}/{image} to {dest}\n".format(pool=self._pool,image=imagename,dest=full_backupname)

    '''
    full backup
    '''	

    def full_backup(self):
	'''
	create full snapshot --> delete overage snapshot --> delete backup export file --> export full snapshot to backup dir
	'''
	print "Starting full backup..."
	for imagename in self._images:
	    print "\033[0;36m"+"{pool}/{image}:".format(pool=self._pool,image=imagename)+"\033[0m"
	    #create full backup snapshot        
            self._create_snapshot(imagename)
	
	    #delete overage snapshot and export backup file
	    if self._get_num_snapshosts(imagename) != 1:
	        full_snapname=self._get_newest_snapshot(imagename)
	        self._delete_overage_snapshot(imagename,full_snapname)
                self._delete_overage_backupfile(imagename)
	    
	    #export full backup	
	    self._export_full_snapshot(imagename)
	    
    def incremental_backup(self):
	'''
	num of snapshot (>7 or ==0) --> full_backup
		        (<7) --> get newest_snapshot --> create cur_snapshot --> export diff-from newest_snapshot image@cur_snapshot file    
	'''

def main():
    cp=cephbackup("rbd","*","/tmp/test","/etc/ceph/ceph.conf",check_mode=False, compress_mode=False, window_size=7, window_unit='days')
    cp.full_backup()    	


if __name__ == '__main__':
    main()








