from weaver import client
c = client.Client('172.31.33.213', 2002) # Syslab
import datetime
colorred = "\033[01;31m{0}\033[00m"
colorgrn = "\033[1;36m{0}\033[00m"
#Creating a new Concept in RoboBrain

def getValFromWeaver(val):
	return val[0]

def mergeProps(l1,l2):
	l2=getValFromWeaver(l2)
	l1=l1.split(',')
	l2=l2.split(',')
	l3=list()
	l3=l1+list(set(l2)-set(l1))
	return ",".join(l3)

def getUpdatedProperty(newProps,oldProps):
	print 'update property'
	print oldProps
	print newProps
	for key,val in newProps.iteritems():
			updateVal=val
			if key in oldProps:
				updateVal=mergeProps(newProps[key],oldProps[key])
			newProps[key]=updateVal

	print newProps
	print 'end of update property'
	return newProps


def InsertConcept(Id='floor',CreatedAt=datetime.datetime.now(),nodeProps={}):
	nodeProps['created_at']=CreatedAt
	if not Id:
		print colorred.format('undefined concept node handle')
		return
	c.begin_tx()
	c.create_node(Id)
	c.set_node_properties(node=Id,properties=nodeProps)
	try:
	    c.end_tx()
	    print 'created a new concept node'
	    node=c.get_node(node=Id)
	except client.WeaverError:
		print 'node already exists'
		nodeProps.pop("created_at", None)
		node=c.get_node(node=Id)
		print node.properties
		oldProps=c.get_node(node=Id).properties
		c.begin_tx()
		c.set_node_properties(node=Id,properties=getUpdatedProperty(nodeProps,oldProps))
		try:
			c.end_tx()
		except client.WeaverError:
			print 'Internal Error, contact the system administrator'

		# c.set_node_properties(node=handle,properties={'feed_ids':feed_ids})
		
		# 	print 'Concept Node already exist:Feed Id inserted'
		# except client.WeaverError:
		# 	print 'Concept Node already exist:Such feed id is already inserted'

#Creating a new Media in RoboBrain
def InsertMedia(Id='random',CreatedAt=datetime.datetime.now(),nodeProps={}):
	nodeProps['created_at']=CreatedAt
	if not Id:
		print colorred.format('undefined media node handle')
		return
	c.begin_tx()
	c.create_node(Id)
	c.set_node_properties(node=Id,properties=nodeProps)
	try:
	    c.end_tx()
	    print 'created a new media node'
            node=c.get_node(node=Id)
	    print node.properties
	except client.WeaverError:
		#c.begin_tx()
		node=c.get_node(node=Id)
		print node.properties
		# c.set_node_properties(node=Id,properties={'feed_ids':feed_ids})
		# try:
		# 	c.end_tx()
		# 	print 'Media Node already exist:Feed Id inserted'
		# except client.WeaverError:
		# 	print 'Media Node already exist:Such feed id is already inserted'

#Creating a new Concept Relationship in RoboBrain
def InsertRelation(CreatedAt=datetime.datetime.now(),src='1',dst='2',edgeDirection='F',edgeProps={}):
	if not src:
		print colorred.format('Source undefined')
		return
	if not dst:
		print colorred.format('Destination undefined')
		return

	edgeProps['created_at']=CreatedAt
	edgeProps['edgeDirection']=edgeDirection
	edges=c.get_edges(node=src,properties={'label':edgeProps['label'],'source_text':edgeProps['source_text'],'source_url':edgeProps['source_url'],'edgeDirection':edgeProps['edgeDirection']},nbrs=[dst])  # all the edges at a particular node
	if len(edges)==1:
		handle=edges[0].handle
		print edges[0].properties
		# c.begin_tx()
		# c.set_edge_properties(node=src,edge=handle,properties={'feed_ids':feed_ids})
		# try:
		# 	c.end_tx()
		# 	edges=c.get_edges(node=src,properties=[('label',label),('keywords',keywords),('source_text',source_text),('source_url',source_url),('edgeDirection',edgeDirection)],nbrs=[dst])  # all the edges at a particular node
		# 	print edges[0].properties
		# except client.WeaverError:
		# 	print 'Edge Exists: Such FeedId Already exists'

	elif len(edges)==0:
		c.begin_tx()
		newEdge=c.create_edge(src,dst)
		c.set_edge_properties(node=src,edge=newEdge,properties=edgeProps)
		try:
			c.end_tx()
			print 'created  new edge'
			edges=c.get_edges(node=src,properties={'label':edgeProps['label'],'source_text':edgeProps['source_text'],'source_url':edgeProps['source_url'],'edgeDirection':edgeProps['edgeDirection']},nbrs=[dst])  # all the edges at a particular node
			print edges[0].properties
		except:
			print 'there is some internal error, contact the system administrator'

	else:
		assert False,'There cannot be two edges which have the exact same properties'


def InsertNewRelation(CreatedAt=datetime.datetime.now(),src='1',dst='2',edgeProps={}):
	InsertRelation(CreatedAt=CreatedAt,src=src,dst=dst,edgeDirection='F',edgeProps=edgeProps)
	InsertRelation(CreatedAt=CreatedAt,src=src,dst=dst,edgeDirection='B',edgeProps=edgeProps)

if __name__ == "__main__":
	InsertConcept(Id='floor',CreatedAt=datetime.datetime.now(),nodeProps={'pk': 'prop1,prop2', 'feed_id': '5576848d76b9a67abccd8073,modified', 'handle': 'floor123', 'prop2': '123', 'label': 'Concept,Image'})
	read_props = c.get_node_properties('floor')
	print read_props
	InsertConcept(Id='wall',CreatedAt=datetime.datetime.now(),nodeProps={'pk': 'prop1', 'feed_id': '5576848d76b9a67abccd8073', 'handle': 'wall', 'prop2': '123', 'label': 'Concept'})
	read_props = c.get_node_properties('wall')
	print read_props
	InsertNewRelation(CreatedAt=datetime.datetime.now(),src='floor',dst='wall',edgeProps={'keywords': 'Human,Affordance,Object,shoe,Standing', 'pk': 'prop1', 'source_text': 'Hallucinating Humans', 'prop2': '123', 'source_url': 'http://pr.cs.cornell.edu/hallucinatinghumans/','label':'hello'})
	
	InsertMedia(Id='random123',CreatedAt=datetime.datetime.now(),nodeProps={'handle': 'heatmap_12', 'mediapath': '/home/ozan/ilcrf/shoe_12_1.jpg_cr.jpg', 'label': 'Media,Image', 'feed_id': '5576848d76b9a67abccd8073', 'prop2': '123', 'pk': 'prop1'})
	

