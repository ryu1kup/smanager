#!/usr/bin/env python3

import time
import glob
import subprocess
import pandas as pd


class smanager():
    def __init__(self, user='', format='%.18i %.9P %.100j %.8u %.2t %.10M %.6D %R'):
        self._user = user
        self._format = format

        smines = subprocess.check_output(['squeue', '-u', user, '-o', format])
        smines = smines.decode('utf8').split('\n')

        ids = [None for _ in range(len(smines) - 2)]
        partitions = [None for _ in range(len(smines) - 2)]
        names = [None for _ in range(len(smines) - 2)]
        users = [None for _ in range(len(smines) - 2)]
        states = [None for _ in range(len(smines) - 2)]
        times = [None for _ in range(len(smines) - 2)]
        nodes = [None for _ in range(len(smines) - 2)]
        nodelists = [None for _ in range(len(smines) - 2)]

        for i, smine in enumerate(smines[1:-1]):
            smine = smine.split(' ')
            s = [s for s in smine if s is not '']
            ids[i] = s[0]
            partitions[i] = s[1]
            names[i] = s[2]
            users[i] = s[3]
            states[i] = s[4]
            times[i] = s[5]
            nodes[i] = s[6]
            nodelists[i] = s[7]
        
        self.df = pd.DataFrame({
            'id': ids,
            'partition': partitions,
            'name': names,
            'user': users,
            'state': states,
            'time': times,
            'node': nodes,
            'nodelist': nodelists,
            })

    def update(self):
        smines = subprocess.check_output(['squeue', '-u', self._user, '-o', self._format])
        smines = smines.decode('utf8').split('\n')

        ids = [None for _ in range(len(smines) - 2)]
        partitions = [None for _ in range(len(smines) - 2)]
        names = [None for _ in range(len(smines) - 2)]
        users = [None for _ in range(len(smines) - 2)]
        states = [None for _ in range(len(smines) - 2)]
        times = [None for _ in range(len(smines) - 2)]
        nodes = [None for _ in range(len(smines) - 2)]
        nodelists = [None for _ in range(len(smines) - 2)]

        for i, smine in enumerate(smines[1:-1]):
            smine = smine.split(' ')
            s = [s for s in smine if s is not '']
            ids[i] = s[0]
            partitions[i] = s[1]
            names[i] = s[2]
            users[i] = s[3]
            states[i] = s[4]
            times[i] = s[5]
            nodes[i] = s[6]
            nodelists[i] = s[7]
        
        self.df = pd.DataFrame({
            'id': ids,
            'partition': partitions,
            'name': names,
            'user': users,
            'state': states,
            'time': times,
            'node': nodes,
            'nodelist': nodelists,
            })

    def show(self):
        self.update()
        print(self.df)

    def count_job(self, pattern=None, state=None):
        self.update()
        df_ = self.df.copy()
        if pattern is not None:
            df_ = df_[df_['name'].str.contains(pattern)]
        if state is not None:
            df_ = df_[df_['state'] == state]
        return len(df_)

    def get_id(self, pattern=None):
        self.update()
        if pattern is None:
            return self.df['id'].tolist()
        else:
            return [id for id, name in zip(self.df['id'], self.df['name']) if pattern in name]

    def submit(self, project_id, limit=100):
        jobs = glob.glob('../mc{}/work/scripts/sbatch*.sh'.format(project_id))
        for job in jobs:
            self.update()
            n = self.count_job()
            if n > limit:
                print('Currently {} batches are submitted. Please wait for 1 min.')
                time.sleep(60)
            else:
                subprocess.run(['sbatch', job])

    def cancel_all(self):
        self.update()
        ids = self.get_id()
        for id in ids:
            subprocess.run(['scancel', id])

    def show_all_activities(self):
        format = '%.18i %.9P %.100j %.8u %.2t %.10M %.6D %R'
        smines = subprocess.check_output(['squeue', '-o', format])
        smines = smines.decode('utf8').split('\n')

        ids = [None for _ in range(len(smines) - 2)]
        partitions = [None for _ in range(len(smines) - 2)]
        names = [None for _ in range(len(smines) - 2)]
        users = [None for _ in range(len(smines) - 2)]
        states = [None for _ in range(len(smines) - 2)]
        times = [None for _ in range(len(smines) - 2)]
        nodes = [None for _ in range(len(smines) - 2)]
        nodelists = [None for _ in range(len(smines) - 2)]

        for i, smine in enumerate(smines[1:-1]):
            smine = smine.split(' ')
            s = [s for s in smine if s is not '']
            ids[i] = s[0]
            partitions[i] = s[1]
            names[i] = s[2]
            users[i] = s[3]
            states[i] = s[4]
            times[i] = s[5]
            nodes[i] = s[6]
            nodelists[i] = s[7]

        df = pd.DataFrame({
            'id': ids,
            'partition': partitions,
            'name': names,
            'user': users,
            'state': states,
            'time': times,
            'node': nodes,
            'nodelist': nodelists,
            })

        vc = df['user'].value_counts()
        for i, v in vc.items():
            print(i, v)



if __name__ == '__main__':
    smanager = smanager(user='ryuichi')
    smanager.show()
