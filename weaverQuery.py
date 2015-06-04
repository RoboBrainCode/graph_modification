from weaver import client
c = client.Client('172.31.33.213', 2002) # Syslab
import datetime
colorred = "\033[01;31m{0}\033[00m"
colorgrn = "\033[1;36m{0}\033[00m"
#Creating a new Concept in RoboBrain
def InsertConcept(Label='Concept',handle='floor',Id='1',CreatedAt=datetime.datetime.now(),feed_ids = 'random123'):
	if not handle:
		print colorred.format('undefined concept node handle')
		return
	c.begin_tx()
	c.create_node(handle)
	c.set_node_properties(node=handle,properties={'labels':':Concept','handle':handle,'created_at':CreatedAt,'feed_ids':feed_ids})
	try:
	    c.end_tx()
	    print 'created a new concept node'
	except client.WeaverError:
		c.begin_tx()
		c.set_node_properties(node=handle,properties={'feed_ids':feed_ids})
		try:
			c.end_tx()
			print 'Concept Node already exist:Feed Id inserted'
		except client.WeaverError:
			print 'Concept Node already exist:Such feed id is already inserted'

#Creating a new Media in RoboBrain
def InsertMedia(Label='Media',handle='unique_handle',Id='random',feed_ids = 'abc',mediatype='Image',mediapath='/path',CreatedAt=datetime.datetime.now()):
	if not handle:
		print colorred.format('undefined media node handle')
		return
	c.begin_tx()
	c.create_node(Id)
	label=':Media:'+mediatype
	c.set_node_properties(node=Id,properties={'labels':label,'handle':handle,'created_at':CreatedAt,'feed_ids':feed_ids,'mediapath':mediapath})
	try:
	    c.end_tx()
	    print 'created a new media node'
	except client.WeaverError:
		c.begin_tx()
		c.set_node_properties(node=Id,properties={'feed_ids':feed_ids})
		try:
			c.end_tx()
			print 'Media Node already exist:Feed Id inserted'
		except client.WeaverError:
			print 'Media Node already exist:Such feed id is already inserted'

#Creating a new Concept Relationship in RoboBrain
def InsertRelation(label='SAME_TYPE',keywords="'Simhat_Torah', 'Rejoicing_in_the_Law', 'synonym', 'wordnet'",source_text='WordNet',source_url='http://wordnet.princeton.edu/',feed_ids=['asdf'],CreatedAt=datetime.datetime.now(),src='1',dst='2',edgeDirection='F'):
	if not src:
		print colorred.format('Source undefined')
		return
	if not dst:
		print colorred.format('Destination undefined')
		return

	edges=c.get_edges(node=src,properties=[('label',label),('keywords',keywords),('source_text',source_text),('source_url',source_url),('edgeDirection',edgeDirection)],nbrs=[dst])  # all the edges at a particular node
	if len(edges)==1:
		handle=edges[0].handle
		print edges[0].properties
		c.begin_tx()
		c.set_edge_properties(node=src,edge=handle,properties={'feed_ids':feed_ids})
		try:
			c.end_tx()
			edges=c.get_edges(node=src,properties=[('label',label),('keywords',keywords),('source_text',source_text),('source_url',source_url),('edgeDirection',edgeDirection)],nbrs=[dst])  # all the edges at a particular node
			print edges[0].properties
		except client.WeaverError:
			print 'Edge Exists: Such FeedId Already exists'

	elif len(edges)==0:
		c.begin_tx()
		newEdge=c.create_edge(src,dst)
		c.set_edge_properties(node=src,edge=newEdge,properties={'label':label,'keywords':keywords,'source_text':source_text,'source_url':source_url,'created_at':CreatedAt,'feed_ids':feed_ids,'edgeDirection':edgeDirection})
		c.end_tx()
		edges=c.get_edges(node=src,properties=[('label',label),('keywords',keywords),('source_text',source_text),('source_url',source_url),('edgeDirection',edgeDirection)],nbrs=[dst])  # all the edges at a particular node
		print edges[0].properties
	else:
		assert False,'There cannot be two edges which have the exact same properties'


def InsertNewRelation(label='SAME_TYPE',keywords="'Simhat_Torah', 'Rejoicing_in_the_Law', 'synonym', 'wordnet'",source_text='WordNet',source_url='http://wordnet.princeton.edu/',feed_ids=['asdf'],CreatedAt=datetime.datetime.now(),src='1',dst='2'):
	return
#	InsertRelation(label=label,keywords=keywords,source_text=source_text,source_url=source_url,feed_ids=feed_ids,CreatedAt=CreatedAt,src=src,dst=dst,edgeDirection='F')
#	InsertRelation(label=label,keywords=keywords,source_text=source_text,source_url=source_url,feed_ids=feed_ids,CreatedAt=CreatedAt,src=dst,dst=src,edgeDirection='B')

if __name__ == "__main__":
	InsertConcept(Label='Concept',handle='floor123',CreatedAt=datetime.datetime.now(),feed_ids = 'random123')
	read_props = c.get_node_properties('floor123')
	print read_props
	InsertConcept(Label='Concept',handle='wall123',CreatedAt=datetime.datetime.now(),feed_ids = 'random')
	read_props = c.get_node_properties('wall123')
	print read_props
	InsertNewRelation(label='SAME_TYPE',keywords="'Simhat_Torah', 'Rejoicing_in_the_Law', 'synonym', 'wordnet'",source_text='WordNet',source_url='http://wordnet.princeton.edu/',feed_ids=['asdf'],CreatedAt=datetime.datetime.now(),src='floor',dst='wall')
	InsertMedia(Label='Media',handle='unique_handle123',Id='random123',feed_ids = 'abc',mediatype='Image',mediapath='/path',CreatedAt=datetime.datetime.now())


