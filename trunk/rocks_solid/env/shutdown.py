'''
Shtudown action
'''

from rocks_solid.env import BaseAction

class Action(BaseAction) :
    def act(self, level='') :
        os.system('/sbin/poweroff')
