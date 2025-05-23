#!/usr/bin/env python3
import pexpect

child = pexpect.spawn('./iris_cli -server 192.168.77.14 -username admin help', timeout=30)
child.expect('Password:', timeout=10)
child.sendline('admin')
child.expect(pexpect.EOF, timeout=20)
output = child.before.decode('utf-8')
print(output) 