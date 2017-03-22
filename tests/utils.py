import subprocess


def execute(cmd, checked):
    if checked:
        func = subprocess.check_call
    else:
        func = subprocess.call
    return func(cmd)


def create_stream(path, checked=False):
    execute(['bash', '-c', 'maprcli stream create -path %s' % path], checked)
    
    
def delete_stream(path, checked=False):
    execute(['bash', '-c', 'maprcli stream delete -path %s' % path], checked)
    
    
def create_topic(stream, topic, partitions=1, checked=False):
    execute(
        ['bash', '-c', 'maprcli stream topic create -path %s -topic %s -partitions %s' % (stream, topic, partitions)],
        checked)
    
    
def delete_topic(stream, topic, checked=False):
    execute(['bash', '-c', "maprcli stream topic delete -path %s -topic %s" % (stream, topic)], checked)
    
    
def new_stream(path, checked=False):
    delete_stream(path, False)
    create_stream(path, checked)
