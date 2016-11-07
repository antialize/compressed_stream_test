import sys
import re

c = sys.stdin.read()

code = '''
file<int> f1;
f1.open(TMP_FILE);

internal_file<int> f2;

std::vector<stream<int>> s1;
std::vector<internal_stream<int>> s2;

int cnt = 0;
'''

for l in c.split('\n'):
	l = l.split('==>')
	if len(l) == 1:
		continue
	l = l[1].strip()
	ns = re.findall(r'\d+', l)
	code += '\n// %s\n' % l;
	s = ns[-1]
	if l.startswith('Create stream '):
		code += 's1.push_back(f1.stream());\n';
		code += 's2.push_back(f2.stream());\n';
	elif l.startswith('Write end '):
		code += 's1[%s].seek(0, whence::end);\n' % s
		code += 's2[%s].seek(0, whence::end);\n' % s
		code += '''\
for (int i = 0; i < %s; i++) {
	s1[%s].write(cnt);
	s2[%s].write(cnt);
	cnt++;
}
''' % (ns[0], s, s)
	elif l.startswith('Read '):
		code += '''\
for (int i = 0; i < %s; i++) {
	ensure(s1[%s].read(), s2[%s].read(), "read");
}
''' % (ns[0], s, s)
	elif l.startswith('Seek start '):
		code += 's1[%s].seek(0, whence::set);\n' %s
		code += 's2[%s].seek(0, whence::set);\n' %s

code = '''
int foo_test() {
	%s

	return EXIT_SUCCESS;
}
''' % code.replace('\n', '\n\t')
print(code)
