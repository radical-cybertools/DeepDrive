#!/usr/bin/env python3


import time

import radical.deepdrive as rd


# ------------------------------------------------------------------------------
#
class DDMD(rd.DeepDrive):

    TASK_TRAIN_MODEL = 'task_train_model'
    TASK_TRAIN_FF    = 'task_train_ff'
    TASK_MD_SIM      = 'task_md_sim'
    TASK_MD_CHECK    = 'task_md_check'
    TASK_DFT         = 'task_dft'

    def __init__(self, cores=16):

        self._threshold =  1

        super().__init__(cores=cores)

        self.register_task_type(ttype=self.TASK_TRAIN_MODEL,
                                on_final=self.control_train_model,
                                glyph='T',
                                descr={'executable': '/bin/sleep',
                                       'arguments' : [3]})

        self.register_task_type(ttype=self.TASK_TRAIN_FF,
                                on_final=self.control_train_ff,
                                glyph='t',
                                descr={'executable': '/bin/sleep',
                                       'arguments' : [3]})

        self.register_task_type(ttype=self.TASK_MD_SIM,
                                on_final=self.control_md_sim,
                                glyph='s',
                                descr={'executable': '/bin/sleep',
                                       'arguments' : [3]})

        self.register_task_type(ttype=self.TASK_MD_CHECK,
                                on_final=self.control_md_check,
                                glyph='c',
                                descr={'executable': '/bin/sleep',
                                       'arguments' : [3]})

        self.register_task_type(ttype=self.TASK_DFT,
                                on_final=self.control_dft,
                                glyph='d',
                                descr={'executable': '/bin/sleep',
                                       'arguments' : [3]})


    # --------------------------------------------------------------------------
    #
    def control_train_model(self, task):
        '''
        react on completed MD simulation task
        '''

        self.dump(task, 'completed model train, next iteration')

        # FIXME: is that the right response?
        self.next_iteration()


    # --------------------------------------------------------------------------
    #
    def control_train_ff(self, task):
        '''
        react on completed ff training task
        '''

        # - When FFTrain task goes away, FFTrain met conversion criteria
        #   - kill MDSim tasks from previous iteration (in next_iteration)
        #   -> CONTINUE WHILE (with new force field)

        self.dump(task, 'completed ff train, next iteration')
        self.next_iteration()


    # --------------------------------------------------------------------------
    #
    def control_md_sim(self, task):
        '''
        react on completed MD sim task
        '''

        # - for any MD that completes
        #   - start UncertaintyCheck test for it (UCCheck)

        tid = task.uid
        self.dump(task, 'completed md, start check ')
        self.submit_tasks(self.TASK_MD_CHECK)


    # --------------------------------------------------------------------------
    #
    def control_md_check(self, task):
        '''
        react on completed MD check task
        '''

        try:
            uncertainty = int(task.stdout.split()[0])
        except:
            uncertainty = 1

        # - if uncertainty > threshold:
        #   - ADAPTIVITY GOES HERE
        #   - run DFT task
        # - else (uncertainty <= threshold):
        #   - MD output -> input to TASK_TRAIN_MODEL
        #   - run new MD task / run multiple MD tasks for each structure
        #     (configurable)

        if uncertainty > self._threshold:
          # self._adaptivity_cb()
            self.submit_tasks(self.TASK_DFT)

        else:
            # FIXME: output to TASK_TRAIN_MODEL
            self.submit_tasks(self.TASK_MD_SIM)


    # --------------------------------------------------------------------------
    #
    def control_dft(self, task):
        '''
        react on completed DFT task
        '''

        # - DFT task output -> input to FFTrain task

        # FIXME: output to TASK_TRAIN_MODEL
        # FIXME: what else us supposed to happen here?
        pass


    # --------------------------------------------------------------------------
    #
    def next_iteration(self):

        self.dump('-----------------------------------------------------------')
        self.dump('next iteration')

        # cancel previous iteration
        self.cancel_tasks()

        # always (re)start a training tasks
        self.submit_tasks(self.TASK_TRAIN_FF   , n=1)
        self.submit_tasks(self.TASK_TRAIN_MODEL, n=1)

        # run initial batch of MD_SIM tasks (assume one core per task)
        self.submit_tasks(self.TASK_MD_SIM, n=self._cores - 2)

        self.dump('next iter: started %s md sims' % (self._cores - 2))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    ddmd = DDMD(cores=8)

    try:
        ddmd.seed(ddmd.TASK_TRAIN_MODEL, 1)
        ddmd.seed(ddmd.TASK_TRAIN_FF,    1)
        ddmd.seed(ddmd.TASK_MD_SIM,     -1)

        ddmd.start()

        while True:
          # ddmd.dump()
            time.sleep(1)

    finally:
        ddmd.close()


# ------------------------------------------------------------------------------

