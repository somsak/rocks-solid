'''
Environmental temperature checker
'''
import re, os

acpi_re = re.compile(r'temperature:\s*(?P<temp>[0-9]).*', re.IGNORECASE)
hddtemp_re = re.compile(r'.*?(?P<temp>[0-9]+).*', re.IGNORECASE | re.DOTALL)

from rocks_solid.env import BaseChecker

class TempChecker(BaseChecker) :
    def __init__(self, config) :
        BaseChecker.__init__(self, config)
        self.ipmi_re = []
        for attr in dir(self.config) :
            if attr.startswith('temp_ipmi_attr') :
                self.ipmi_re.append(re.compile(getattr(config, attr), re.IGNORECASE))
        try :
            self.thereshold = self.config.temp_thereshold
        except :
            self.thereshold = 35

    def check(self) :
        temp = None
        # IPMI first
        ipmi_cmd= os.popen('ipmitool -I open sdr type temperature 2> /dev/null', 'r')
        while 1 :
            line = ipmi_cmd.readline()
            if not line: break
            fields = line.strip().split('|')
            name = fields[0].strip()
            for r in self.ipmi_re :
#                print name
#                print r.pattern
                if r.match(name) :
                    value = fields[len(fields) - 1].strip()
                    if self.config.verbose :
                        print 'IPMI temperature = %s' % value
                    try :
                        temp = int(value.split()[0])
                        break
                    except :
                        pass
        ipmi_cmd.close()

        # ACPI
        if temp is None :
            try :
                for dir in os.listdir('/proc/acpi/thermal_zone') :
                    path = os.path.join('/proc/acpi/thermal_zone', dir, 'temperature')
                    f = open(path, 'r')
                    line = f.read().strip()
                    f.close()
                    m = acpi_re.match(line)
                    if m :
                        if self.config.verbose :
                            print 'ACPI temperature = %s' % line
                        temp = int(m.group('temp'))
                        break
            except :
                pass

        # HDDTemp
        if temp is None :
            try :
                try :
                    hddtemp = config.env_hddtemp
                    drive = config.env_hddtemp_arg
                except :
                    hddtemp = '/usr/sbin/hddtemp'
                    drive = '/dev/sda'
                
                if os.access(hddtemp, os.X_OK) :
                    cmd = os.popen('%s %s' % (hddtemp, drive), 'r')
                    output = cmd.read().strip()
                    temp_str = output.split(':')[2]
                    if self.config.verbose :
                        print 'HDDTemp output = %s' % output
                        print 'Temperature string = %s' % temp_str             
                    m = hddtemp_re.match(temp_str)
                    if m :
                        temp = int(m.group('temp'))
                    cmd.close()
            except :
                pass

        if self.config.verbose :
            print 'Temperature = %s' % temp
        # what next? maybe lm_sensor?
        if temp is None :
            return 0
        elif temp <= self.thereshold :
            return 0
        else :
            return temp - self.thereshold

Checker = TempChecker

if __name__ == '__main__' :
    from rocks_solid import config_read

    config = config_read()
    setattr(config, 'verbose', True)
    t = TempChecker(config)
    print t.check()
