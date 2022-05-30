import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = df.set_index(df['assignment_group'])
df.drop(columns=['assignment_group','total_user','total_click'],inplace=True)
df

#for colormap
new_df = df.T
new_df['c'].astype('float')
new_df['t'].astype('float')
new_df['e'] =  ((((new_df['t'])/new_df['c'])-1)).astype('float')
new_df
G = nx.Graph()

for metric in df.columns:
    G.add_node(metric, c=df[metric]['c'], t=df[metric]['t'])


#manual edges and pos
G.add_edge('rpu', 'revenue_per_click', symbol='=')
G.add_edge('revenue_per_click', 'click_per_user', symbol='*')
G.add_edge('click_per_user', 'ctr')
G.add_edge('ctr', 'view_per_user', symbol='*')



pos = {'rpu': np.array([0  , 0]),
     'revenue_per_click': np.array([1  ,  0]),
     'click_per_user': np.array([2  , 0]),
     'ctr': np.array([2  , -1]),
     'view_per_user': np.array([3  , -1])
      }

#for labeling
nodes = list(G.nodes)
edge_labels = nx.get_edge_attributes(G,'symbol')
control = nx.get_node_attributes(G,'c')
treatment = nx.get_node_attributes(G,'t')

labels = {}
for k in nodes:
    changes= round(((treatment[k] - control[k]) / control[k] ) * 100,2)
    labels[k] = '{0} \nC :{1} \nT :{2} \nE :{3}%'.format(k, control[k], treatment[k], changes)

from matplotlib.colors import LinearSegmentedColormap
cmap=LinearSegmentedColormap.from_list('rg',["r","w","g"], N=5) 
vmin=-0.05
vmax=0.05
plt.figure(figsize=(30,10));
plt.axis('equal');
nx.draw(G, with_labels=True, labels=labels, font_weight='bold', pos=pos, node_shape='s', edgecolors='k' ,node_size=10000, node_color=new_df['e'], cmap=cmap, vmin=vmin, vmax=vmax)
nx.draw_networkx_edge_labels(G, pos, edge_labels = edge_labels, font_size=20);
sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin = vmin, vmax=vmax))
sm._A = []
cbar=plt.colorbar(sm);
cbar.set_label('Effect_Size_Changes', rotation=270)

periscope.output(plt)