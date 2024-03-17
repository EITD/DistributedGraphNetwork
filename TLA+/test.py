import concurrent.futures
from time import sleep

messages = ['v8v10v20f20', 'v0v10v20f20', 'v8v10f10', 'v8f0', 'v0v10f10','marker_0_8', 'v0f0', 'marker_0_0']
K = 3

out_edges_list = ['8', '0']
inbox = {v:[] for v in out_edges_list}


neighbor_features = [[] for _ in range(K)]
Enabled = out_edges_list.copy()


sp = [1]

id = 1




def handle_msg(c, messageList):
    global Enabled, neighbor_features, inbox, sp, id
    while True:
        try:
            message = messageList.pop(0)

            if "marker_" in message:
                Enabled.remove(c)

            else:
                index = message.count('v')
                neighbor_features[index - 1].append(message)

                send_feature = f"v{id}" + message
                neighbor_feature_list = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for out in out_edges_list:
                        future = executor.submit(ask, out, send_feature)
                        neighbor_feature_list.append(future)
                concurrent.futures.wait(neighbor_feature_list)
        except IndexError:
            print('neighbor_features', neighbor_features)
            sleep(3)
            continue
        except Exception as e:
            print(e)


def khop_neighborhood():
    return 1

def ask(out, send_feature):
    return 'ok'






executor = concurrent.futures.ThreadPoolExecutor()
for c in out_edges_list:
    future = executor.submit(handle_msg, c, inbox[c])
    fList.append(future)



for message in messages:
    toInbox(message)
    print('messages', messages)
    print('inbox', inbox)
    print('Enabled', Enabled)

concurrent.futures.wait(fList)





# if len(Enabled) == 0:
#                     vertex_feature_list = []
#                     with concurrent.futures.ThreadPoolExecutor() as executor:
#                         for out in out_edges_list:
#                             future = executor.submit(ask, out, f"v{id}f1")
#                             vertex_feature_list.append(future)
#                     concurrent.futures.wait(vertex_feature_list)

#                     sp.append(khop_neighborhood())
#                     neighbor_features = [[] for _ in range(K)]

#                     vertex_marker_list = []
#                     with concurrent.futures.ThreadPoolExecutor() as executor:
#                         for out in out_edges_list:
#                             future = executor.submit(ask, out, f"marker_{epoch}_{id}")
#                             vertex_marker_list.append(future)
#                     concurrent.futures.wait(vertex_marker_list)

#                     Enabled = out_edges_list

# if "snapshot_" in message:
#             _, epoch = message.split("_")
#             epoch = parts[1]
            
#             self.record(self.get(self.epoch()))

#             # pass feature and then marker
#             for e in range(epoch):
#                 initial_vertex_feature_list = []
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#                     for out in self.out_edges_list:
#                         future = executor.submit(ask, out, f"v{self.id}f{self.get(self.epoch())}")
#                         initial_vertex_feature_list.append(future)
#                 concurrent.futures.wait(initial_vertex_feature_list)

#                 initial_vertex_marker_list = []
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#                     for out in self.out_edges_list:
#                         future = executor.submit(ask, out, f"marker_{e}_{self.id}")
#                         initial_vertex_marker_list.append(future)
#                 concurrent.futures.wait(initial_vertex_marker_list)