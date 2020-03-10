from gensim.models import Word2Vec
import networkx as nx
import os
import logging
import json
import pickle


basedir = '/home/uddin11e/Desktop/dblp-ref/dblp-ref-'
outputdir = '/home/uddin11e/Desktop/dblp_outputs/'

# dblpfile= open('/mnt/3A0F13EC43ACDB83/workspace/dblp-ref/dblp-ref-0.json')
# titles = dblpfile.readlines()
# dblpfile.close()
# for x in range(10):
#     doc = json.loads(titles[x])
#     print(doc["id"])

title_list = []
list_for_graph = []
dg = nx.DiGraph()
page_ranks = {}

# parse data file and create list
def parseAllJson(file_path):
   f = open(file_path)
   lines = f.readlines()
   f.close()
   for line in lines:
       doc = json.loads(line)
       title_list.append(doc.get("title"))
       list_for_graph.append([doc.get("id"),doc.get("references")])
# create directed graph    
def create_graph(data_list):
    for item in data_list:
        node = item[0]
        edges = item[1]
        dg.add_node(node)
        if edges != None:
            for edge in edges:
                dg.add_edge(node,edge)



for i in range (4):
    parseAllJson(basedir+str(i)+".json")
    # with open(outputdir+'dblpv10_titles', 'w+') as f:
    #     f.writelines(title_list)
    # for i in range(10):
    #     print(list_for_graph[i][0])
print('json parsed')
create_graph(list_for_graph)
nx.write_gpickle(dg,outputdir+'paper_graph.gpickle')
    # dg = nx.read_gpickle(outputdir+'paper_graph.gpickle')
    # print('read graph into memory')
    # with open(outputdir+'paper_graph.gpickle','r') as f:
        # dg = nx.read_gpickle(f)
print('page ranking..')
page_ranks = nx.pagerank(dg, alpha=0.85, tol=1.0e-6, nstart=None, weight=1, dangling=None)
ranks = list(page_ranks.values())
with open(outputdir+'/page_ranks.txt','w+') as f:
    for key in page_ranks:
        f.write(key+" "+str(page_ranks[key])+'\n')

print('page rank writen to file')





print("total title",len(title_list))
 
