import unittest
import os
import json
import check_and_download as cad

from unittest.mock import patch
from pprint import pprint

class TestMyCode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = [
            {
                "name": "3Pi",
                "ip": "10.210.35.27",
                "ptstdir": "/home/acwh025/Documents/PTST",
                "outputdir": "/Volumes/kaleem_ssd/phd_data/3Pi/ML"
            },
            {
                "name": "5Pi",
                "ip": "10.210.58.126",
                "ptstdir": "/home/acwh025/Documents/PTST",
                "outputdir": "/Volumes/kaleem_ssd/phd_data/5Pi/ML_DATA"
            }
        ]
        with open('test_controller.json', 'w') as f:
            json.dump(data, f)
            
    @classmethod
    def tearDownClass(cls):
        os.remove('test_controller.json')
    
    def test_ping_machine(self):
        machines = cad.get_machines('test_controller.json')
        for machine in machines:
            can_ping_machine = cad.ping_machine(machine['ip'])
            self.assertNotEqual(can_ping_machine, None)
    
    def test_validate_machines(self):
        machines = cad.get_machines('test_controller.json')
        for machine in machines:
            is_machine_valid = cad.validate_machine(machine)
            self.assertNotEqual(is_machine_valid, None)
    
    def test_get_machines(self):
        with self.assertRaises(ValueError) as cm:
            machines = cad.get_machines(None)
        
        machines = cad.get_machines('test_controller.json')
        
        for machine in machines:
            # ? Empty machines
            self.assertNotEqual(len(machine.keys()), 0)
            
            # ? Need the 4 keys
            self.assertEqual(len(machine.keys()), 4)
            
            # ? The 3 keys have to name, ip, ptstdir, outputdir
            required_keys = ["ip", "name", "ptstdir", "outputdir"]
            are_keys_matching = all(key in machine.keys() for key in required_keys)
            self.assertEqual(are_keys_matching, True)
        
        self.assertNotEqual(machines, None)
        self.assertNotEqual(len(machines), 0)
        
if __name__ == '__main__':
    unittest.main()