#!/usr/bin/python3
import subprocess
import os
import sys
import csv
import codecs
import mysql.connector
import socket, struct
import tempfile
import shutil
#import fcntl
import traceback
import re
import config

def get_script_path():
	return os.path.dirname(os.path.realpath(sys.argv[0]))

temp_dir_path = tempfile.mkdtemp()
script_path = get_script_path()
script_get_vm_list = os.path.join(script_path, "list_vms_ext.ps1")

def tag_string_escape(s):
	s = s.replace(",", ":"
	    ).replace("@", "%"
	    ).replace("!", " "
	    ).replace("#", " "
	    ).replace("$", " "
	    ).replace("^", " "
	    ).replace("&", " "
	    ).replace("*", " "
	    ).replace("-", " "
	    ).replace("(", " "
	    ).replace(")", " "
	    ).replace("_", " "
	    ).replace("=", " "
	    ).replace("|", " "
	    ).replace("\\", " "
	    ).replace("[", " "
	    ).replace("]", " "
	    ).replace("{", " "
	    ).replace("}", " "
	    ).replace("/", " "
	    ).replace(";", " "
	    ).replace("'", " "
	    ).replace('"', " "
	    ).replace('«', " "
	    ).replace('»', " ")

	s = re.sub(r' +', " ", s)
	return s

def eprint(*args, **kwargs):
	print(*args, file=sys.stderr, **kwargs)

def ip2num(ip):
	try:
		return struct.unpack("!L", socket.inet_aton(ip))[0]

	except OSError:
		return 0

def remove_if_exist(path):
	try:
		os.remove(path)

	except FileNotFoundError:
		pass

def csv_to_dicts(csv_path):
	reader = csv.reader(codecs.open(csv_path, 'rU', 'utf-16'), delimiter=',', quotechar='"')
	tmp = []
	for row in reader:
		tmp.append(row)

	if len(tmp) < 2:
		return []

	keys = tmp[1]
	tmp = tmp[2:] #First line garbage, second line titles
	dicts = []
	for t in tmp:
		dicts.append(dict(zip(keys, t)))

	return dicts

def get_value_from_db(db_cnx, query, args):
	ret = 0
	cursor = db_cnx.cursor()
	cursor.execute(query, args)
	row = cursor.fetchone()
	if row is not None:
		ret = row[0]

	cursor.close()
	return ret

def get_object_id(db_cnx, object_type_id, vm_name):
	return get_value_from_db(db_cnx, "SELECT id FROM Object WHERE objtype_id=%s AND name=%s", [object_type_id, vm_name])

def check_ip_in_database(db_cnx, vm_ip):
	ret = []
	cursor = db_cnx.cursor()
	cursor.execute("SELECT object_id FROM IPv4Allocation WHERE ip=%s", [ip2num(vm_ip)])
	row = cursor.fetchone()
	while row is not None:
		ret.append(row[0])
		row = cursor.fetchone()

	cursor.close()
	return ret

def get_hardwired_type_id(db_cnx):
	return get_value_from_db(db_cnx, "SELECT id FROM PortInnerInterface WHERE iif_name='hardwired'", [])

def get_ethernet_type_id(db_cnx):
	return get_value_from_db(db_cnx, "SELECT id FROM PortOuterInterface WHERE oif_name='virtual port'", [])

def get_object_type_id(db_cnx, object_type_chapter_id, type_name):
	return get_value_from_db(db_cnx, "SELECT dict_key FROM Dictionary WHERE dict_value=%s and chapter_id=%s", [type_name, object_type_chapter_id])

def get_object_type_chapter_id(db_cnx):
	return get_value_from_db(db_cnx, "SELECT * FROM Chapter WHERE name='ObjectType'", [])

def get_attribute_id(db_cnx, name):
	return get_value_from_db(db_cnx, "SELECT id FROM Attribute WHERE name=%s", [name])

def put_row_to_database(db_cnx, query, args):
	cursor = db_cnx.cursor()
	cursor.execute(query, args)
	db_cnx.commit()
	cursor.close()
	return

def put_row_to_database_get_id(db_cnx, query, args):
	ret = 0
	cursor = db_cnx.cursor()
	cursor.execute(query, args)
	ret = cursor.lastrowid
	db_cnx.commit()
	cursor.close()
	return ret

def put_object_to_database(db_cnx, object_type_id, name, notes):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO Object(name, label, objtype_id, asset_no, has_problems, comment) VALUES(%s, NULL, %s, NULL, 'no', %s)", [name, object_type_id, notes])

def assign_ip(db_cnx, object_id, ip):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO IPv4Allocation(object_id, ip, name, type) VALUES(%s, %s, 'generated' , 'regular')", [object_id, ip2num(ip)])

def clear_ips(db_cnx, object_id):
	put_row_to_database(db_cnx, "DELETE FROM IPv4Allocation WHERE object_id=%s", [object_id])
	return

def assign_int_attribute(db_cnx, attr_id, vm_id, object_type_id, value):
	put_row_to_database(db_cnx, "INSERT INTO AttributeValue(object_id, object_tid, attr_id, string_value, uint_value, float_value) VALUES(%s, %s, %s, NULL, %s, NULL) ON DUPLICATE KEY UPDATE uint_value=%s", [vm_id, object_type_id, attr_id, value, value])
	return

def get_chapter_id(db_cnx, name):
	put_row_to_database(db_cnx, "INSERT IGNORE INTO Chapter(sticky, name) VALUES ('no', %s)", [name])
	return get_value_from_db(db_cnx, "SELECT id FROM Chapter WHERE name=%s", [name])

def put_dictionary_value(db_cnx, chapter_id, value):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO Dictionary(chapter_id, dict_sticky, dict_value) VALUES(%s, 'no', %s)", [chapter_id, value])

def get_dictionary_id(db_cnx, chapter_id, name):
	return get_value_from_db(db_cnx, "SELECT dict_key FROM Dictionary WHERE chapter_id=%s AND dict_value=%s AND dict_sticky='no'", [chapter_id, name])

def map_dictionary(db_cnx, object_type_id, chapter_id, attr_id):
	put_row_to_database(db_cnx, "INSERT INTO AttributeMap(objtype_id, attr_id, chapter_id, sticky) VALUES(%s, %s, %s, 'no') ON DUPLICATE KEY UPDATE chapter_id=%s", [object_type_id, attr_id, chapter_id, chapter_id])
	return

def create_dict_attribute(db_cnx, name):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO Attribute(type, name) VALUES('dict', %s)", [name])

def assign_tag_as_attribute(db_cnx, object_type_id, object_id, tag_category, tag_value):
	tag_chapter_id = get_chapter_id(db_cnx, tag_category)
	if tag_chapter_id == 0:
		raise OSError

	tag_dict_id = get_dictionary_id(db_cnx, tag_chapter_id, tag_value)
	if tag_dict_id == 0:
		tag_dict_id = put_dictionary_value(db_cnx, tag_chapter_id, tag_value)

	if tag_dict_id == 0:
		raise OSError

	attr_id = get_attribute_id(db_cnx, tag_category)
	if attr_id == 0:
		attr_id = create_dict_attribute(db_cnx, tag_category)
		if attr_id == 0:
			raise OSError

	map_dictionary(db_cnx, object_type_id, tag_chapter_id, attr_id)
	assign_int_attribute(db_cnx, attr_id, object_id, object_type_id, tag_dict_id)
	return

def rename_object(db_cnx, object_id, name):
	put_row_to_database(db_cnx, "UPDATE Object SET name=%s WHERE id=%s", [name, object_id])
	return

def get_entity_link_parent(db_cnx, child_object_id):
	return get_value_from_db(db_cnx, "SELECT parent_entity_id FROM EntityLink WHERE parent_entity_type='object' AND child_entity_type='object' AND child_entity_id=%s LIMIT 1", [child_object_id])

def link_entity(db_cnx, parent_object_id, child_object_id):
	parent_current = get_entity_link_parent(db_cnx, child_object_id)
	if parent_current == 0:
		put_row_to_database(db_cnx, "INSERT INTO EntityLink(parent_entity_type, parent_entity_id, child_entity_type, child_entity_id) VALUES ('object', %s, 'object', %s)", [parent_object_id, child_object_id])
	else:
		if parent_current != parent_object_id:
			put_row_to_database(db_cnx, "UPDATE EntityLink SET parent_entity_id=%s WHERE parent_entity_type='object' AND child_entity_type='object' AND child_entity_id=%s", [parent_object_id, child_object_id])

	return

def get_tag_id(db_cnx, name):
	return get_value_from_db(db_cnx, "SELECT id FROM TagTree WHERE tag=%s", [name]);

def create_tag(db_cnx, parent_tag_id, assignable, name):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO TagTree(parent_id, is_assignable, tag) VALUES(%s, %s, %s)", [parent_tag_id, 'yes' if assignable else 'no', name])

def assign_tag_to_object(db_cnx, tag_id, object_id):
	put_row_to_database(db_cnx, "INSERT IGNORE INTO TagStorage (entity_realm, entity_id, tag_id, tag_is_assignable, user, date) VALUES ('object', %s, %s, 'yes', 'magic script', NOW())", [object_id, tag_id])
	return

def assign_tag_as_tag(db_cnx, tag_head, tag_tail, object_id):
	tag_head = tag_string_escape(tag_head)
	tag_tail = tag_string_escape(tag_tail)
	tag_head_id = get_tag_id(db_cnx, tag_head)
	if tag_head_id == 0:
		tag_head_id = create_tag(db_cnx, None, False, tag_head)

	tag_tail_id = get_tag_id(db_cnx, tag_tail)
	if tag_tail_id == 0:
		tag_tail_id = create_tag(db_cnx, tag_head_id, True, tag_tail)

	assign_tag_to_object(db_cnx, tag_tail_id, object_id)
	return

def assign_port(db_cnx, object_id, port_inner_type_id, port_outer_type_id, l2address, num):
	return put_row_to_database_get_id(db_cnx, "INSERT INTO Port (object_id, name, iif_id, type, l2address) VALUES(%s, %s, %s, %s, %s)",
	                                  [object_id, "interface" + str(num), port_inner_type_id, port_outer_type_id, l2address.replace(":", "").upper()])

def clear_ports(db_cnx, object_id):
	put_row_to_database(db_cnx, "DELETE FROM Port WHERE object_id=%s", [object_id])
	return

def clear_tags(db_cnx, object_id):
	put_row_to_database(db_cnx, "DELETE FROM TagStorage WHERE entity_id=%s", [object_id])
	return

def import_from_vsphere(vm_uuid):
	cnx = mysql.connector.connect(user=config.db_user,
                                      password=config.db_password,
                                      host=config.db_host,
                                      #unix_socket=config.db_unix_socket,
                                      database=config.db_name)

	if (vm_uuid):
		subprocess.check_call(['powershell', '-File', script_get_vm_list, '-outDir', temp_dir_path, '-vcsahost', config.vcsahost, '-username', config.username, '-password', config.password, '-vmid', vm_uuid])
	else:
		subprocess.check_call(['powershell', '-File', script_get_vm_list, '-outDir', temp_dir_path, '-vcsahost', config.vcsahost, '-username', config.username, '-password', config.password])

	vms = csv_to_dicts(os.path.join(temp_dir_path, "list.csv"))
	object_type_chapter_id = get_object_type_chapter_id(cnx)
	if object_type_chapter_id == 0:
		raise OSError

	vm_type_id = get_object_type_id(cnx, object_type_chapter_id, "VM")
	if vm_type_id == 0:
		raise OSError

	cluster_type_id = get_object_type_id(cnx, object_type_chapter_id, "VM Cluster")
	if cluster_type_id == 0:
		raise OSError

	_cluster_id = config.cluster_id
	if _cluster_id == 0:
		_cluster_id = get_object_id(cnx, cluster_type_id, config.cluster_str)

	if _cluster_id == 0:
		raise OSError

	port_inner_type_id = get_hardwired_type_id(cnx)
	if port_inner_type_id == 0:
		raise OSError

	port_outer_type_id = get_ethernet_type_id(cnx)
	if port_outer_type_id == 0:
		raise OSError

	ram_attr_id = get_attribute_id(cnx, config.ram_attr_str)
	if ram_attr_id == 0:
		raise OSError

	hdd_attr_id = get_attribute_id(cnx, config.hdd_attr_str)
	if hdd_attr_id == 0:
		raise OSError

	cpu_threads_attr_id = get_attribute_id(cnx, config.cpu_threads_attr_str)
	if cpu_threads_attr_id == 0:
		raise OSError

	for row in vms:
		vm = {}
		vm['name'] = row['Name']
		vm['notes'] = row['Notes']
		#vm['guest'] = row['Guest']
		vm['cpu_num'] = row['NumCpu']
		vm['ram'] = row['MemoryGB']
		#vm['host_ip'] = row[8]
		#vm['folder'] = row[11]
		vm['id'] = row['PersistentId']
		vm['hdd'] = row['ProvisionedSpaceGB']
		guestOS = row['Guest']
		guestOS = re.sub(r'^.*:', "", guestOS)
		print("name=", vm['name'])
		#print("host_ip=", vm['host_ip'])
		print("id=", vm['id'])

		ethernet_adapters = csv_to_dicts(os.path.join(temp_dir_path, 'macs-' + vm['id'] + ".csv"))
		macs = []
		for ethernet in ethernet_adapters:
			macs.append(ethernet['MacAddress'])

		vm['macs'] = macs
		#for mac in macs:
		#	print("mac=", mac)

		tag_info = csv_to_dicts(os.path.join(temp_dir_path, 'tags-' + vm['id'] + ".csv"))
		tags = []
		for tag_info_row in tag_info:
			tags.append([tag_info_row['Category'], tag_info_row['Name'], tag_info_row['Description']])

		vm['tags'] = tags
		#for tag in tags:
		#	print("tag=", tag)

		ip_info = csv_to_dicts(os.path.join(temp_dir_path, 'ips-' + vm['id'] + ".csv"))
		ips = []
		for ip_info_row in ip_info:
			if len(ip_info_row) > 1:
				if ip_info_row['IP Address'] != "":
					ips.append(ip_info_row['IP Address'])

		vm['ips'] = ips
		#for ip in ips:
		#	print("ip=", ip)
		
		vm_object_id = get_object_id(cnx, vm_type_id, vm['name'])
		if get_entity_link_parent(cnx, vm_object_id) != _cluster_id:
			rename_object(cnx, vm_object_id, vm['name'] + "_renamed_by_script")
			eprint("VM renamed: " + vm['name'] + " -> " + vm['name'] + "_renamed_by_script")
			vm_object_id = 0

		if vm_object_id == 0:
			print("Adding VM to database")
			vm_object_id = put_object_to_database(cnx, vm_type_id, vm['name'], vm['notes'])

		if vm_object_id == 0:
			#print("ERROR: vm_object_id is 0!")
			raise OSError

		assign_int_attribute(cnx, ram_attr_id, vm_object_id, vm_type_id, int(round(float(vm['ram'].replace(',','.')))))
		assign_int_attribute(cnx, cpu_threads_attr_id, vm_object_id, vm_type_id, vm['cpu_num'].replace(',','.'))
		assign_int_attribute(cnx, hdd_attr_id, vm_object_id, vm_type_id, int(round(float(vm['hdd'].replace(',','.')))))
		clear_tags(cnx, vm_object_id)
		clear_ports(cnx, vm_object_id)
		if len(vm['ips']) > 0:
			clear_ips(cnx, vm_object_id)

		for tag in vm['tags']:
			#print("tag[2]='" + tag[2] + "'")
			tag_full = tag[1] + ((", " + tag[2]) if tag[2] else "")
			#print("Tag assigning: " + tag[0] + " / " + tag_full)
			if tag[0] in config.attribute_tags:
				assign_tag_as_attribute(cnx, vm_type_id, vm_object_id, tag[0], tag_full)
			else:
				assign_tag_as_tag(cnx, tag[0], tag_full, vm_object_id)

		if guestOS:
			assign_tag_as_tag(cnx, 'OS', guestOS, vm_object_id)

		link_entity(cnx, _cluster_id, vm_object_id)
		for ip in vm['ips']:
			ip_object_ids = check_ip_in_database(cnx, ip)
			if len(ip_object_ids) == 0:
				assign_ip(cnx, vm_object_id, ip)
				print("IP address ", ip, " must be assigned")
			elif not (vm_object_id in ip_object_ids):
				assign_ip(cnx, vm_object_id, ip)
				eprint("Warning: other object used ip " + ip);

		num = 0
		for mac in vm['macs']:
			num = num + 1
			assign_port(cnx, vm_object_id, port_inner_type_id, port_outer_type_id, mac, num)

	cnx.close();

vm_uuid = ""
if len(sys.argv) == 2:
	vm_uuid = sys.argv[1]

#lock_file_path = os.path.join(tempfile.gettempdir(), 'getinfofromvsphere.lock');
#try:
#	lock_file = open(lock_file_path, 'w+')
#	fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
try:
	import_from_vsphere(vm_uuid)
except:
	traceback.print_exc(file=sys.stdout)

#	fcntl.flock(lock_file, fcntl.LOCK_UN)
#	lock_file.close()
#except:
#	traceback.print_exc(file=sys.stdout)

#os.remove(lock_file_path)
shutil.rmtree(temp_dir_path)
