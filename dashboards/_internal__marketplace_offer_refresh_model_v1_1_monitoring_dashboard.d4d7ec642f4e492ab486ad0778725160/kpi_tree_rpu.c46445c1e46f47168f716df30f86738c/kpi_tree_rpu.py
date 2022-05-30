import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def tree_for_breakdown(metric_breakdown_df):
  	
    metric_breakdown_df = metric_breakdown_df.loc[:,['assignment_group','partner','rpu']]
    parent_metric = metric_breakdown_df[['assignment_group','rpu']].groupby('assignment_group').sum()

    child_metric = metric_breakdown_df.pivot(index='assignment_group', columns='partner')
    child_metric.columns = child_metric.columns.get_level_values(1)

    df = pd.concat([parent_metric,child_metric], axis=1)

    #add node
    G = nx.Graph()
    for metric in df.columns:
        G.add_node(metric, c=df[metric]['c'], t=df[metric]['t'])

    parent_node = parent_metric.columns[0]

    for child_node in child_metric.columns:
        G.add_edge(parent_node, child_node)

    #add pos for tree view
    pos = {}

    pos[parent_node] = ((len(child_metric.columns)-1)/2.0,0)

    for i,child_node in enumerate(child_metric.columns):
        pos[child_node] = (i,-1)

    #for colormap
    new_df = df.T
    new_df['c'].astype('float')
    new_df['t'].astype('float')
    new_df['e'] =  ((((new_df['t'])/new_df['c'])-1)).astype('float')

    nodes = list(G.nodes)
    edge_labels = nx.get_edge_attributes(G,'symbol')
    control = nx.get_node_attributes(G,'c')
    treatment = nx.get_node_attributes(G,'t')

    labels = {}
    for k in nodes:
        changes= round(((treatment[k] - control[k]) / control[k] ) * 100,2)
        labels[k] = '{0} \nC :{1} \nT :{2} \nE :{3}%'.format(k, control[k], treatment[k], changes)

    #plot
    from  matplotlib.colors import LinearSegmentedColormap
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
    
tree_for_breakdown(df)

periscope.output(plt)