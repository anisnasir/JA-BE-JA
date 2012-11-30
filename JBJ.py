import random
import sys
import pycassa

from random import choice
from random import sample
from pycassa.index import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime import datetime

t1 = datetime.now()
pool = ConnectionPool('JBJ')
col_fam_master = pycassa.ColumnFamily(pool, 'Master')

delta = 0.003
T0 = 2

#Function: Load graph from a file
def get_color_degree(graph,index,color):
	neighbors = list(graph[index][1])
	energy = 0
	for neighbor in neighbors:
		if graph[neighbor][0] == color:
			energy +=1
	return energy
	
def get_neighbor(graph, index):
	temp = list(graph[index][1])
	return temp[random.randint(0,len(temp)-1)]
	
def get_view(graph, index,distance):
	array =[]
	while distance != 0:
		temp = list(graph[index][1])
		index = temp[random.randint(0,len(temp)-1)]
		distance -=1
		array.append(index)
	return array

def swap(graph,index1,index2,Tr):
	cp = graph[index1][0]
	cq = graph[index2][0]
	xpcp = get_color_degree(graph,index1,cp)
	xqcq = get_color_degree(graph,index2,cq)
	xpcq = get_color_degree(graph,index1,cq)
	xqcp = get_color_degree(graph,index2,cp)
	if ((xpcq*xpcq + xqcp*xqcp)*Tr) > ((xpcp*xpcp)+(xqcq*xqcq)):
		graph[index1][0] = cq
		graph[index2][0] = cp
		return True
	else:
		return False
		
def sample_and_swap(graph,index,distance):
	print distance
	try:
		Tr = graph[index][2]
		candidateNode = get_neighbor(graph,index)
		if not swap(graph,index,candidateNode,Tr):
			view = get_view(graph,index,distance)
			for candidateNode in view:
				if swap(graph,index,candidateNode,Tr):
					break
		graph[index][2] = Tr - delta
		if(graph[index][2] <1):
			graph[index][2] =1
	except:
		print "Unexpected error:", sys.exc_info()[0]
	

def UIS_WOR(graph,count):
	return sample(graph,count)
	
def load_graph(fname,total_server):
	server=0
    	try:
		fr = open(fname, 'r')
		G = {}   
		for line in fr:

			if not line.startswith('#'):
				a,b = map(int, line.split())    
			    	if a not in G:  
					G[a] = [server,set(),T0]
					server= (server+1)%total_server
			    	if b not in G:  
					G[b] = [server%total_server,set(),T0]
					server= (server+1)%total_server
			    	G[a][1].add(b)
			    	G[b][1].add(a)    
		fr.close()    
		return G
	except:
		return 0
if __name__ == '__main__':
	try:
		G = load_graph("edges_500.txt",4)
		iterations = 1000
		while iterations >0:
			uis_wor = UIS_WOR(G,len(G))
			for i in uis_wor:
				print i
				sample_and_swap,(G,i,10)
			iterations -=1
	except:
		print "Unexpected error:", sys.exc_info()[0]
	
	for i in G:
		col_fam_master.insert(str(i),{'server':G[i][0]})
	

	t2 = datetime.now()

	delta = t2 - t1
	print "total time elapsed: ",str(delta)[:-4]

