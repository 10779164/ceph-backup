#!encoding=utf8
#!/usr/bin/python	
from  executor import execute
import re
import time
import argparse
import os
import rados
import rbd

class cephbackup(object):
    '''
    rbd commands:
    rbd snap create rbd/image@snap_name
    rbd export-diff --from snap newest_snap rbd/image@cur_snap /path/filename
    '''
    PREFIX = 'SNAPSHOT'
    TIME_FMT = time.strftime('%Y%m%d%H%M%S')
    SNAPSHOT_NAME = '{}-{}'.format(PREFIX,TIME_FMT)


    def __init__(self, pool, images, backup_dest, ceph_conf, backup_init):
        super(cephbackup, self).__init__()
 	self._pool=pool
	self._images = images
        self._backup_dest = backup_dest
	self._ceph_conf= ceph_conf
	self._backup_init = backup_init

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


    def _backup_init_whether(self, imagename):
	num = int(self._backup_init)+1
	num_snapshots = int(self._get_num_snapshosts(imagename))
	if num_snapshots > num:
	    return False
	else:
	    return True


    def _delete_overage_snapshot(self,imagename,full_snapname):
	snapshots = self._get_snapshots(imagename)
	image = rbd.Image(self._ceph_ioctx,imagename)
	for overage_snapshot in snapshots:
	    if overage_snapshot != full_snapname: 
	        print "Deleting snapshot {pool}/{snapname}".format(pool=self._pool,snapname=overage_snapshot)
		image.remove_snap(overage_snapshot)


    def _delete_overage_backupfile(self,imagename):
	#snapshots = self._get_snapshots(imagename)
	dest_dir = os.path.join(self._backup_dest, self._pool, imagename)
	for dest_file in os.listdir(dest_dir):
	    if re.match(r"{image}@(.*?)".format(image=imagename),dest_file):
		backup_file = dest_dir+'/'+dest_file
		print "Deleting backup file {backup_file}".format(backup_file=dest_file)
		os.remove(backup_file)


    def _export_full_backupfile(self,imagename):
	filename=imagename+'@'+cephbackup.SNAPSHOT_NAME+".full"
	dest_dir=os.path.join(self._backup_dest, self._pool, imagename)
    	if not os.path.exists(dest_dir):
	    os.makedirs(dest_dir)
	full_filename=dest_dir+'/'+filename	
	execute("rbd export {pool}/{image} {dest}".format(pool=self._pool,image=imagename,dest=full_filename),sudo=True)
	print "Exporting image {pool}/{image} to {dest}\n".format(pool=self._pool,image=imagename,dest=full_filename)


    def _export_diff_backupfile(self,imagename,newest_snapshot,cur_snapshot):
	filename=imagename+'@'+cur_snapshot+".diff_from_"+newest_snapshot
	dest_dir=os.path.join(self._backup_dest, self._pool, imagename)
	if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
	full_filename=dest_dir+'/'+filename
	execute("rbd export-diff --from-snap {base} {pool}/{image}@{cur} {dest}".format(base=newest_snapshot,pool=self._pool,image=imagename,cur=cur_snapshot,dest=full_filename),sudo=True)
	print "Exporting image {pool}/{image} to {dest}\n".format(pool=self._pool,image=imagename,dest=full_filename)


    def incremental_full_backup(self, imagename):
	self._create_snapshot(imagename)
	full_snapname=self._get_newest_snapshot(imagename)	
	self._delete_overage_snapshot(imagename,full_snapname)
        self._delete_overage_backupfile(imagename)
        self._export_full_backupfile(imagename)


    '''
    def full_backup(self):
	#create full snapshot --> delete overage snapshot --> delete backup export file --> export full snapshot to backup dir
	print "Starting full backup..."
	for imagename in self._images:
	    print "\033[0;36m"+"{pool}/{image}:".format(pool=self._pool,image=imagename)+"\033[0m"
	    #create full backup snapshot        
            self._create_snapshot(imagename)
	
	    #delete overage snapshot and export backup file
	    if self._get_num_snapshosts(imagename) != 1 and self._backup_mode == "incremental":
	        full_snapname=self._get_newest_snapshot(imagename)
	        self._delete_overage_snapshot(imagename,full_snapname)
                self._delete_overage_backupfile(imagename)
		self._export_full_backupfile(imagename)
	    
	    else:
	    #export full backup	
	        self._export_full_backupfile(imagename)
    '''

    #Ceph rbd full backup
    def full_backup(self):
	'''
        create full snapshot --> delete overage snapshot --> delete backup export file --> export full snapshot to backup dir
        '''
	print "Starting full backup..."
	for imagename in self._images:
	    print "\033[0;36m"+"{pool}/{image}:".format(pool=self._pool,image=imagename)+"\033[0m"	
	    self._create_snapshot(imagename)
	    self._export_full_backupfile(imagename)


    #Ceph rbd incremental backup		    
    def incremental_backup(self):
	'''
	num of snapshot (>7 or ==0) --> full_backup
		        (<7) --> get newest_snapshot --> create cur_snapshot --> export diff-from newest_snapshot image@cur_snapshot file    
	'''
	print "Starting increment backup..."
	for imagename in self._images:
	    m=self._backup_init_whether(imagename)
	    if m:
	        print "\033[0;36m"+"Starting incremental backup for {image}:".format(image=imagename)+"\033[0m"
	    
	        #get current newest snapshot
	        newest_snapshot = self._get_newest_snapshot(imagename)		

	        #create snapshot
	        cur_snapshot = self._create_snapshot(imagename)

	        #export diff file
	        self._export_diff_backupfile(imagename,newest_snapshot,cur_snapshot)

 	    else:
		print "\033[0;36m"+"Starting new full backup for {image}:".format(image=imagename)+"\033[0m"
		self.incremental_full_backup(imagename)


def test():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pool', help="Ceph rbd pool name", required=True)
    parser.add_argument('-i', '--images', nargs='+', help="List of ceph images to backup", required=True)
    parser.add_argument('-d', '--dest', help="Backup file directory", required=True)
    parser.add_argument('-c', '--ceph-conf', help="Path to ceph configuration file", type=str, default='/etc/ceph/ceph.conf')
    args = parser.parse_args()

    cp=cephbackup(args.pool, args.images, args.dest, args.ceph_conf, args.backup_mode)
    cp.incremental_backup()


def main():
    cp=cephbackup("rbd","*","/tmp/test/","/etc/ceph/ceph.conf",3)
    #cp.full_backup()
    cp.incremental_backup()    	


if __name__ == '__main__':
    main()
    #test()








