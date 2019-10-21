#from typing import Dict
#encoding: utf-8
import time
import rados
import rbd
from kubernetes import client, config
import commands
import re
import os, shutil



class ClusterCeph(object):

    def __init__(self, stack_name, directory, full_name=None):
        self._PREFIX = 'CloudCluster-Snapshot'
        self._TIME_FMT = time.strftime('%Y%m%d%H%M%S')
        self._SNAPSHOT_NAME = '{}-{}'.format(self._PREFIX, self._TIME_FMT)

        self._cephconf = "/etc/ceph/ceph.conf"
        self._kubeconf = "/root/.kube/config"
        self._pool = "rbd"
        self._stack_name = stack_name
        self._directory = directory
        self._full_name = full_name
        self._image_name = self._get_imagename()

        try:
            cluster = rados.Rados(conffile=self._cephconf)
            cluster.connect()
        except:
            raise Exception('Unable to connect to ceph cluster')
        else:
            self._ceph_ioctx = cluster.open_ioctx(self._pool)
            # self._ceph_rbd = rbd.RBD()


    def _get_imagename(self):
        config.load_kube_config(self._kubeconf)
        v1 = client.CoreV1Api()
        volume = []
        for i in v1.list_namespaced_persistent_volume_claim(self._stack_name).items:
            volume.append(i.spec.volume_name)
        image = []
        for i in v1.list_persistent_volume().items:
            if i.metadata.name in volume:
                image.append(i.spec.rbd.image)
        print image
        return image
        # list


    def _create_snapshot(self, imagename):
        snapshot_name = self._SNAPSHOT_NAME
        image = rbd.Image(self._ceph_ioctx, imagename)
        image.create_snap(snapshot_name)
        return snapshot_name


    def _delete_snapshot(self, snapshot_name, imagename):
        image = rbd.Image(self._ceph_ioctx, imagename)
        image.remove_snap(snapshot_name)


    def _get_snapshot(self, imagename):
        # return a dic
        # {'namespace': 0, 'id': 266L, 'name': u'SNAPSHOT-20190530004929', 'size': 1073741824L}
        # dic.get('name') return snapshot name
        prefix_length = len(self._PREFIX)
        image = rbd.Image(self._ceph_ioctx, imagename)
        snapshot = []
        for i in image.list_snaps():
            if i.get('name')[0:prefix_length] == self._PREFIX:
                snapshot.append(i.get('name'))
        return snapshot[0]


    def _get_num_snapshot(self,imagename):
        return len(self._get_snapshot(imagename))


    def _exist_full_snapshot_whether(self, imagename):
        # have full snaphot return True; else return False
        m = self._get_num_snapshot(imagename)
        if m == 0:
            return False
        else:
            return True


    def _export_full_backupfile(self, imagename):
        file_name = imagename + '-' + self._SNAPSHOT_NAME + '.full'
        dest_dir = os.path.join(self._directory, self._pool, imagename)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        dir = dest_dir + '/' + file_name
        commands.getoutput("rbd export {pool}/{image} {dest}".format(pool=self._pool, image=imagename, dest=dir))


    def _export_discrepancy_backupfile(self, imagename, cur_snapshot, full_snapshot):
        file_name = imagename + '-' + cur_snapshot + ".diff_from_" + full_snapshot
        dest_dir = os.path.join(self._directory, self._pool, imagename)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        dir = dest_dir + '/' + file_name
        commands.getoutput(
            "rbd export-diff --from-snap {base} {pool}/{image}@{cur} {dest}".format(base=full_snapshot, pool=self._pool,
                                                                                    image=imagename, cur=cur_snapshot,
                                                                                    dest=dir))



    #########restore
    def _get_pod_name(self):
        config.load_kube_config(self._kubeconf)
        v1 = client.CoreV1Api()
        for i in v1.list_namespaced_pod(self._stack_name).items:
            pod_name = i.metadata.name
            break
        return pod_name


    def _get_pod_status(self,pod_name):
        config.load_kube_config(self._kubeconf)
        v1 = client.CoreV1Api()
        try:
            pod_status = v1.read_namespaced_pod_status(pod_name,self._stack_name).status.phase
        except:
            pod_status = "404"
            return pod_status
        else:
            return pod_status


    def _get_statefulset_name(self):
        config.load_kube_config(self._kubeconf)
        v1 = client.AppsV1Api()
        for i in v1.list_namespaced_stateful_set(self._stack_name).items:
            statefulset_name = i.metadata.name
        return statefulset_name


    def _get_statefulset_replicas(self):
        config.load_kube_config(self._kubeconf)
        v1 = client.AppsV1Api()
        for i in v1.list_namespaced_stateful_set(self._stack_name).items:
            replicas=i.spec.replicas
        return replicas


    def _stop_pod(self, statefulset_name):
        cmd = "kubectl scale statefulset/{statefulset} --replicas=0 -n {namespace}".format(statefulset=statefulset_name,namespace=self._stack_name)
        commands.getoutput(cmd)
        pod_name = self._get_pod_name()
        while True:
            try:
                pod_status = self._get_pod_status(pod_name)
                if pod_status == '404':
                    break
            except:
                time.sleep(2)
                continue


    def _start_pod(self, replicas, statefulset_name):
        cmd = "kubectl scale statefulset/{statefulset} --replicas={replicas} -n {namespace}".format(statefulset=statefulset_name,replicas=replicas,namespace=self._stack_name)
        commands.getoutput(cmd)
        while True:
            try:
                pod_name = self._get_pod_name()
                pod_status = self._get_pod_status(pod_name)
                if pod_status == 'Running':
                    break
            except:
                time.sleep(2)
                continue


    def _rename_image(self, image_name):
        new_image_name = image_name + '-' + self._TIME_FMT + '.bak'
        cmd = "rbd rename {image_name} {new_image_name}".format(image_name=image_name,new_image_name=new_image_name)
        commands.getoutput(cmd) 



    def backup(self):
        """
        :param directory: 备份文件存储目录
        :param full_name: 全备份的文件名，如果传了这个值表示做差异备份，否则做全量
        :return: 备份后的文件文件的路径和备份类型
        """
        backup_type = "discrepancy" if self._full_name else "full"
        for imagename in self._image_name:
            if backup_type == "full":
                m = self._exist_full_snapshot_whether(imagename)
                if m:
                    full_snapshot_name = self._get_snapshot(imagename)
                    self._delete_snapshot(full_snapshot_name, imagename)
                    self._create_snapshot(imagename)  #create fullsnapshot
                    self._export_full_backupfile(imagename)
                else:
                    self._create_snapshot(imagename)
                    self._export_full_backupfile(imagename)
            else:
                full_snapshot = str(self._get_snapshot(imagename)[0])
                cur_snapshot = str(self._create_snapshot(imagename))
                self._export_increment_backupfile(imagename,cur_snapshot,full_snapshot)
                self._delete_snapshot(cur_snapshot,imagename)



        '''
        res = {
            "return_code": 0,
            "description": "description",
            "backup_type": backup_type,
            "backup_file": "/somepath/somefile.text"
        }
        return res
        '''

        '''
        res = {
            "return_code": 0,
            "description": "description",
            "backup_type": backup_type,
            "backup_file": {"image1":"filename1","image2":"filename2","image3":"filename3"}
        }
        return res
        '''

    def restore(self, image_name, full_name, discrepancy_name=None):
        """
        还原备份文件
        :param directory: 备份文件存储目录
        :param full_name: 全备文件
        :param discrepancy_name:  差异文件
        :return:
        """


        statefulset_name = self._get_statefulset_name()
        statefulset_replicas = self._get_statefulset_replicas()

        #stop pod
        self._stop_pod(statefulset_name)

        #rename pod image
        self._rename_image(image_name)

        #restore
        if discrepancy_name == None:
            fullname = self._directory+'/'+self._pool+'/'+image_name+'/'+full_name
            cmd = "rbd import {fullname} {pool}/{image} --image-format 2 --image-feature layering".format(fullname=fullname, pool=self._pool, image=image_name)
            commands.getoutput(cmd)
        else:
            #restore full backup
            fullname = self._directory+'/'+self._pool+'/'+image_name+'/'+full_name
            cmd1 = "rbd import {fullname} {pool}/{image} --image-format 2 --image-feature layering".format(fullname=fullname, pool=self._pool, image=image_name)
            commands.getoutput(cmd1)
            
            #create init snapshot
            cmd2 = "echo"+" "+full_name+" | awk -F "+image_name+"- "+"{'print $2'} | awk -F '.full' {'print $1'}"
            snapshot_name = commands.getoutput(cmd2) 
            image = rbd.Image(self._ceph_ioctx, image_name)
            image.create_snap(snapshot_name)
            
            #restore discrepancy backup
            discrepancyname = self._directory+'/'+self._pool+'/'+image_name+'/'+ discrepancy_name
            cmd3 = "rbd import-diff {discrepancyname} {image_name}".format(discrepancyname=discrepancyname, image_name=image_name)
            commands.getoutput(cmd3)

        	#start pod
            self._start_pod(statefulset_replicas, statefulset_name)
        



        res = {
            "return_code": 0,
            "description": "Success"
        }
        return res

    def delete_backup(self, directory, file_names):
        """
        :param directory: 备份文件存储目录
        :param file_paths: 文件列表
        """
        res = {
            "return_code": 0,
            "description": "Failure"
        }
        return res


a = ClusterCeph('wordpress-2174','/home/backup')
#a.backup()
a.restore('kubernetes-dynamic-pvc-31df97be-eeee-11e9-96af-06f6e321bcf2','kubernetes-dynamic-pvc-31df97be-eeee-11e9-96af-06f6e321bcf2-CloudCluster-Snapshot-20191020214738.full')

