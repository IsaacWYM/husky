#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "base/log.hpp"
#include "boost/tokenizer.hpp"
#include "core/engine.hpp"
#include "io/input/line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_manager.hpp"
#include "core/channel/channel_store.hpp"
#include "utility"
#include "vector"

using namespace std;

// class Vertex;

class VertexObj;

// class TriangleObj;

namespace husky {
    namespace base {
        husky::BinStream& operator<<(husky::BinStream& stream, const VertexObj& v);
        husky::BinStream& operator>>(husky::BinStream& stream, VertexObj& v);
    }
}

// class Vertex {
//    public:
//     using KeyT = int;
//     KeyT object_id;
//     KeyT node_id;
//     const KeyT& id() const { return object_id; }

//     vector<int> neighbours;

//     Vertex(){};
//     // explicit Vertex(const KeyT& init_node_id, vector<pair<int, int> > edge_list): object_id(init_node_id),
//     // node_id(init_node_id){

//     // }
//     explicit Vertex(const KeyT& init_node_id) : object_id(init_node_id), node_id(init_node_id) {};

//     // Serialization and deserialization
//     friend husky::BinStream& ::husky::base::operator<<(husky::BinStream& stream, const Vertex& v) {
//         // husky::base::log_msg("hello in serial\n");
//         stream << v.object_id << v.node_id << v.neighbours;
//         return stream;
//     }
//     friend husky::BinStream& ::husky::base::operator>>(husky::BinStream& stream, Vertex& v) {
//         // husky::base::log_msg("hello in deserial\n");
//         stream >> v.object_id >> v.node_id >> v.neighbours;
//         return stream;
//     }

//     // // Serialization and deserialization
//     // friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
//     //     husky::base::log_msg("hello in serial\n");
//     //     stream << v.object_id << v.node_id << v.excess << v.to_sink_cap << v.to_sink_flow << v.fulfill_gap << v.neighbours << v.incident_triangles << v.back_flow_residual;
//     //     return stream;
//     // }
//     // friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
//     //     husky::base::log_msg("hello in deserial\n");
//     //     stream >> v.object_id >> v.node_id >> v.excess >> v.to_sink_cap >> v.to_sink_flow >> v.fulfill_gap >> v.neighbours >> v.incident_triangles << v.back_flow_residual;
//     //     return stream;
//     //}
// };

struct Edge {
    int vert_a;
    int vert_b;
    Edge() = default;
    Edge(int a, int b) : vert_a(a), vert_b(b) {};
};

struct FlowEdge {
    int neig_id;
    int neig_height;
    double edge_cap;

    FlowEdge() = default;
    FlowEdge(int id, int height, double cap): neig_id(id), neig_height(height), edge_cap(cap) {};
};

struct Flow{
    int incoming_id;
    double incoming_flow;

    Flow() = default;
    Flow(int vid, double flow): incoming_id(vid), incoming_flow(flow) {};
};

struct HeightChange{
    int vid;
    int height;

    HeightChange() = default;
    HeightChange(int id, int height): vid(id), height(height) {};
};


class VertexObj {
    public:
        using KeyT = int;
        KeyT obj_id;
        const KeyT& id() const { return obj_id; }

        double excess;
        int height;
        vector<int> neighbours;
        // vector<int> neig_height;
        // vector<double> edge_cap;
        vector<FlowEdge> edges;
        int push_flag;

        VertexObj(){};

        explicit VertexObj(const KeyT& init_node_id) : obj_id(init_node_id) { 
            excess = 0; push_flag = -1; height = 0; 
            // if (init_node_id == 0){
            //     // tailored setting for the source
            //     push_flag = 0;
            // }
            // no need, the source also need to act as a black hole so even it has excess, no push or relabel would happen for that
        };

        void buildEdges(int vid, int height, double cap){
            // neighbours.push_back(vid);
            // neig_height.push_back(height);
            // edge_cap.push_back(cap);
            edges.push_back(FlowEdge(vid, height, cap));
        }

        // Serialization and deserialization
       friend husky::BinStream& ::husky::base::operator<<(husky::BinStream& stream, const VertexObj& v) {
            // husky::base::log_msg("hello in serial\n");
            stream << v.obj_id << v.excess << v.height << v.neighbours << v.edges << v.push_flag;
            return stream;
        }
        friend husky::BinStream& ::husky::base::operator>>(husky::BinStream& stream, VertexObj& v) {
            // husky::base::log_msg("hello in deserial\n");
            stream >> v.obj_id >> v.excess >> v.height >> v.neighbours >> v.edges << v.push_flag;
            return stream;
        }

};


void triangle_listing() {
    husky::io::LineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    // parameter setting
    float subgraph_size = 2.0;
    // husky::base::log_msg("tri_den_lb: " + to_string(tri_den_lb) + ", tri_den_ub: " + to_string(tri_den_ub) + ", tri_den_attempt: " + to_string(tri_den_attempt));

    auto& vertex_list = husky::ObjListStore::create_objlist<VertexObj>();
    // husky::globalize(vertex_list);
   
    //auto& triangle_set_list = husky::ObjListStore::create_objlist<TriangleSet>();

    int vertex_total = 0;

    auto parse_graph_input = [&vertex_list, &vertex_total](boost::string_ref& neighbour_list) {
        if (neighbour_list.size() == 0)
            return;

        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(neighbour_list, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator iter = tok.begin();

        int node_id = stoi(*iter++);
        VertexObj v(node_id);
        while (iter != tok.end()) {
            v.neighbours.push_back(stoi(*iter));
            iter++;
        }

        vertex_list.add_object(std::move(v));
        vertex_total++;
        // triangle_set_list.add_object(std::move(TriangleSet(node_id)));
        //husky::base::log_msg(string("push vertex with id ") + to_string(node_id));
    };

    husky::load(infmt, parse_graph_input);

    husky::base::log_msg("after loading the graph");

    // to distribute the vertex object over different threads (base on the default hashing of C++)
    husky::globalize(vertex_list);

    /* for checking whether reading the data correctly
       for (int i = 0; i < vertice_list.size(); i++)
       {
       Vertex ver = vertice_list[i];
       cout << "Vertex id: " << ver.id() << endl;
       cout << "neighbours: ";
       for (int j = 0; j < ver.neighbours.size(); j++)
       {
       cout << ver.neighbours[j] << " ";
       }
       cout << endl;
       }
    //*/

    // initialise the channel from Block objects to TriangleSet objects
    //auto& counting_channel =
    //    husky::ChannelStore::create_push_channel<Triangle>(block_list, triangle_set_list);

    auto& edge_push_channel = husky::ChannelStore::create_push_channel<Edge>(vertex_list, vertex_list);


    // *************** sending the edge information ***************
    //*
    cout << "start listing triangles\n";
    husky::list_execute(vertex_list, [&](VertexObj& vertex) {

        // the neighbours are ordered
        // track down the position of the firt neighbour with larger id than the one of the vertex itself.
        int neig_start = 0;
        while (neig_start < vertex.neighbours.size() && vertex.neighbours[neig_start] < vertex.id()) { neig_start++; }

        // send to the vertices with messages with larger id() in a way that triangle ABC would ONLY be send by A to C in the form (A, B)
        // And upon receiving message, vertex C would check whether B is one of its neigbour. No need to check A as it is guaranteed.

        if (neig_start < vertex.neighbours.size()){
            for (int vert_c_ctr = neig_start + 1; vert_c_ctr < vertex.neighbours.size(); vert_c_ctr++){
                for (int vert_b_ctr = neig_start; vert_b_ctr < vert_c_ctr; vert_b_ctr++){
                    edge_push_channel.push(Edge(vertex.id(), vertex.neighbours[vert_b_ctr]), vertex.neighbours[vert_c_ctr]);
                    // husky::base::log_msg("Edge (" + to_string(vertex.id()) + ", " + to_string(vertex.neighbours[vert_b_ctr]) + ") push to vertex " + to_string(vertex.neighbours[vert_c_ctr]));
                }
            }
        }

    });

    /*
    husky::list_execute(vertex_list, [&](Vertex& vertex){
        vector<Edge> received_edges = edge_push_channel.get(vertex);
        for (auto edge: received_edges){
            cout << "Vertex " << vertex.id() << " receive the edge ( " << edge.vert_a << " , " << edge.vert_b << ") on thread " << husky::Context::get_global_tid() << " \n";
        }
    });
    */

    
    
    // *|*|*|*|*|*|*|* matching triangles *|*|*|*|*|*|*|*

    // use Aggregator to get the sum of triangles TIMELY, and assign id to them
    husky::lib::Aggregator<int> num_triangle(0, [](int& a, const int& b){ a += b; });
    auto& ac = husky::lib::AggregatorFactory::get_channel();
    auto& push_triangle_channel = husky::ChannelStore::create_push_channel<int>(vertex_list, vertex_list);

    // match the edges and push the triangles to a new list
    vector<husky::ChannelBase*>chs = {&ac, &push_triangle_channel};
    int thread_id = husky::Context::get_global_tid();
    int thread_total = husky::Context::get_num_global_workers();
    int thread_ctr = 0;
    husky::list_execute(vertex_list, {&edge_push_channel}, chs, [&](VertexObj& vertex){
        vector<Edge> received_edges = edge_push_channel.get(vertex);
        for (int edge_ctr = 0; edge_ctr < received_edges.size(); edge_ctr++){
            // cout << "Vertex " << vertex.id() << " receive the edge ( " << received_edges[edge_ctr].vert_a << " , " << received_edges[edge_ctr].vert_b << ") on thread " << husky::Context::get_global_tid() << " \n";
            
            // checking if the second vertex is a neighbour
            // can be replaced with a binary search
            // int vert_b_id = received_edges[edge_ctr].vert_b;
            for (int neig_ctr = 0; neig_ctr < vertex.neighbours.size(); neig_ctr++){
                if (vertex.neighbours[neig_ctr] == received_edges[edge_ctr].vert_b){
                    //cout << "Triangle (" << received_edges[edge_ctr].vert_a << " " << received_edges[edge_ctr].vert_b << " " << vertex.id() << ") found in thread " << thread_id << "\n";
                    // husky::base::log_msg("Triangle (" + to_string(received_edges[edge_ctr].vert_a) + " " + to_string(received_edges[edge_ctr].vert_b) + " " 
                    //     + to_string(vertex.id()) + ") found in thread " + to_string(thread_id) + " , with id " + to_string(thread_ctr * thread_total + thread_id) + "\n");
                    num_triangle.update(1);
                    int new_tri_id = thread_ctr * thread_total + thread_id + vertex_total + 2; // the number vertex_total + 1 is reserved for the sink's id

                    // the edges from vertices to triangle vertices
                    push_triangle_channel.push(new_tri_id, received_edges[edge_ctr].vert_a);
                    push_triangle_channel.push(new_tri_id, received_edges[edge_ctr].vert_b);
                    push_triangle_channel.push(new_tri_id, vertex.id());

                    // the edges from triangle vertices to vertices
                    push_triangle_channel.push(received_edges[edge_ctr].vert_a, new_tri_id);
                    push_triangle_channel.push(received_edges[edge_ctr].vert_b, new_tri_id);
                    push_triangle_channel.push(vertex.id(), new_tri_id);

                    thread_ctr++;
                    break;
                }
            }
            
        }
    });

    husky::base::log_msg("num_triangle updated to " + to_string(num_triangle.get_value()));


    // set the lower bound and the upbound of the expected triangle density
    double tri_den_lb = ((float)num_triangle.get_value() * 3) / vertex_total;
    double tri_den_ub = 3 * (vertex_total - 1) * (vertex_total - 2) / 6.0;
    double tri_den_attempt = 3 * 3.2;
    husky::base::log_msg("Now attempting to achieve a triangle density of " + to_string(tri_den_attempt));

    

    husky::list_execute(vertex_list, [&](VertexObj& v){
        vector<int> received_ids = push_triangle_channel.get(v);

        // string info = "Vertex " + to_string(v.id()) + " received: ";
        // for (auto id: received_ids) info += (to_string(id) + " ");
        // husky::base::log_msg(info);
        // now its neigbours are the triangle vertices (if it has some previously)
        v.neighbours.clear();

        // init flow -  a flow of value of # incident triangles
        if (v.id() <= vertex_total) {
            // vertex
            v.excess = received_ids.size();
            // edge to source (with id 0), and record its height (#vertices)
            v.buildEdges(0, vertex_total, v.excess);
            // edge to sink (with id #vertices + 1), and record its height (0)
            v.buildEdges(vertex_total + 1, 0, tri_den_attempt);
            // edges are all towards triangle vertices, all with the initial height 0
            for (auto tri_id: received_ids){
                v.buildEdges(tri_id, 0, 1);
            }

            v.push_flag = 1;
            v.height = 1;

        } else {
            // triangle vertex
            v.excess = 0;

            // edges are all towards vertices, all with height 1 as they are assumed lifted to triggger future flows
            for (auto vert_id: received_ids){
                v.buildEdges(vert_id, 1, 2);
            }

            v.push_flag = 0;
            v.height = 0;
        }

        // order the edges according the the lexicopgrahic ordering (height, id) - as the admissable edges with the proper height would all be listed at the front
        sort(v.edges.begin(), v.edges.end(), [](FlowEdge fe1, FlowEdge fe2){
            if (fe1.neig_height < fe2.neig_height) return true;
            else if (fe1.neig_height == fe2.neig_height) {
                if (fe1.neig_id < fe2.neig_id) return true;
                else return false;
            }

            return false;
        });
    });

    // Is it necessary?
    husky::globalize(vertex_list);

    
    // for checking the initial flow network is well set
    /*
    husky::list_execute(vertex_list, [&](VertexObj& v){
        string info = "Vertex " + to_string(v.id()) + " :\n";
        for (int i = 0; i < v.edges.size(); i++) {
            info += ("edge to vertex " + to_string(v.edges[i].neig_id) + " with height " + to_string(v.edges[i].neig_height) + " and capacity " + to_string(v.edges[i].edge_cap) + "\n");
        }
        husky::base::log_msg(info);
    });
    //*/


    // set initial height, push_flag for the source and the sink
    // for safety, set the sink vertex's push_flag to -1 so it will never be involved in anything
    // also ready for implementing a heuristic on deciding a good intial height estimate
    auto& set_height_push_flag_channel = husky::ChannelStore::create_push_channel<int>(vertex_list, vertex_list);
    set_height_push_flag_channel.push(vertex_total, 0);
    set_height_push_flag_channel.push(0, vertex_total + 1);

    husky::list_execute(vertex_list, [&](VertexObj& v){});

    husky::list_execute(vertex_list, [&](VertexObj& v){
        vector<int> height_setting = set_height_push_flag_channel.get(v);
        if (height_setting.size() == 1) {
            v.height = height_setting[0];
        } else if (height_setting.size() > 1){
            husky::base::log_msg("duplicate height setting for Vertex " + to_string(v.id()));
        }
    });
    

    int iteration = 0; 
    // int iter_limit = 1000;
    int iter_limit = stoi(husky::Context::get_param("iter"));
    auto& push_flow_channel = husky::ChannelStore::create_push_channel<Flow>(vertex_list, vertex_list);
    // int excess_push_vertex_count = vertex_total;
    husky::lib::Aggregator<int> excess_push_vertex_count(0, [](int& a, const int& b){ a += b; });
    husky::lib::Aggregator<int> max_to_lift_height(0, [](int& a, const int& b){ if (b > a) { a = b; } });
    auto& push_height_change_channel = husky::ChannelStore::create_push_channel<HeightChange>(vertex_list, vertex_list);

    excess_push_vertex_count.to_reset_each_iter();
    max_to_lift_height.to_reset_each_iter();

    bool need_to_push = true;
    while (iteration < iter_limit && need_to_push){

        husky::base::log_msg("Start Iteration " + to_string(iteration));
        // push until no more is applicable
        // bool need_to_push = true;
        while (need_to_push) {

            need_to_push =  false;
            husky::list_execute(vertex_list, [&](VertexObj& v){
                if (v.excess > 1e-10 && v.push_flag == 1){
                    int admissable_edge_pos = 0;
                    // find the start of edges with proper height
                    while (admissable_edge_pos < v.edges.size() && v.edges[admissable_edge_pos].neig_height < (v.height - 1)) admissable_edge_pos++;
                    // continue to find the first such edges with non-zero capacity (can be none and would stop after passing all edges of proper height)
                    while (admissable_edge_pos < v.edges.size() && v.edges[admissable_edge_pos].neig_height == (v.height - 1) && v.edges[admissable_edge_pos].edge_cap < 1e-10) admissable_edge_pos++;

                    // perform pushing
                    for (int pos = admissable_edge_pos; pos < v.edges.size(); pos++){
                        if (v.edges[pos].neig_height == (v.height - 1)) {
                            double push_amount = 0;
                            if (v.edges[pos].edge_cap > v.excess){
                                push_amount = v.excess;
                            } else{
                                push_amount = v.edges[pos].edge_cap;
                            }

                            push_flow_channel.push(Flow(v.id(), push_amount), v.edges[pos].neig_id);
                            husky::base::log_msg("Vertex " + to_string(v.id()) + " push " + to_string(push_amount) + " unit flow to Vertex " + to_string(v.edges[pos].neig_id));
                            // update the cap and excess
                            v.excess -= push_amount;
                            v.edges[pos].edge_cap -= push_amount;
                            if (v.excess < 1e-10) {
                                // husky::base::log_msg("Vertex " +  to_string(v.id()) + " is inactive now");
                                break;
                            }
                        } else break; // the following edges are all of larger height, which is not admissable
                    }

                    // if all push-able edges are used up and still with excess, set push-flag to 0 for notifying relabel
                    if (v.excess > 0){
                        v.push_flag = 0;
                    }
                }
            });

            // received flow and update excess and edge cap accordingly
            husky::list_execute(vertex_list, {&push_flow_channel}, {&ac}, [&](VertexObj& v){
                vector<Flow> received_flow = push_flow_channel.get(v);
                // special handle for the sink
                if (v.id() == (vertex_total + 1) || v.id() == 0) {
                    for (auto flow: received_flow){
                        v.excess += flow.incoming_flow;
                    }
                    return;
                }
                for (auto flow: received_flow){
                    // vector<Flow>::iterator edge_pos = find(v.edges.begin(), v.edges.end(), flow.incoming_id);
                    // can be improved to binary search here
                    int edge_pos = 0;
                    while (edge_pos < v.edges.size() && v.edges[edge_pos].neig_id != flow.incoming_id) edge_pos++;

                    // update the cap and excess
                    v.excess += flow.incoming_flow;
                    v.edges[edge_pos].edge_cap += flow.incoming_flow;
                    husky::base::log_msg("Vertex " + to_string(v.id()) + " received " + to_string(flow.incoming_flow) + " unit flow from Vertex " + to_string(flow.incoming_id));
                }

                if (v.excess > 1e-10){
                    if (v.push_flag == 1) excess_push_vertex_count.update(1);
                    else if (v.push_flag == 0) {
                        husky::base::log_msg("Vertex " + to_string(v.id()) + " need to be lifted");
                        max_to_lift_height.update(v.height);
                    }
                } 
            });


            if (excess_push_vertex_count.get_value() != 0) need_to_push = true;
            

        }

        husky::base::log_msg("Finished iterative pushing");
        husky::base::log_msg("Largest vertex height to be lifted: " +  to_string(max_to_lift_height.get_value()));

        /*    
        husky::list_execute(vertex_list, [&](VertexObj& v){
            husky::base::log_msg("Vertex " + to_string(v.id()) + " (Height " + to_string(v.height) + ") :");
            string info = "";
            for (int i = 0; i < v.edges.size(); i++) {
                info += ("edge to vertex " + to_string(v.edges[i].neig_id) + "(height " + to_string(v.edges[i].neig_height) + ")," + " capacity: " + to_string(v.edges[i].edge_cap) + "\n");
            }
            info += ("excess: " + to_string(v.excess) + " push_flag: " + to_string(v.push_flag) + "\n");
            husky::base::log_msg(info);
        });
        */


        int record_max_to_lift_height = max_to_lift_height.get_value();
        husky::base::log_msg("Start lifting, and record_max_to_lift_height: " + to_string(record_max_to_lift_height));
        // *|*|*|*|*|*|*|* all possible pushes are applied and now it is time to relabel *|*|*|*|*|*|*|*  

        // propose a change of height
        // note that two adjacent vertices with same height can propose a change according to each other
        husky::list_execute(vertex_list, [&](VertexObj& v){
            if ((v.height == record_max_to_lift_height) && (v.excess > 1e-10) && (v.id() != (vertex_total + 1)) && (v.id() != 0)){
    
                // find the necessary height to lift: min height among admissable edges
                int edge_pos = 0;
                for (; edge_pos < v.edges.size(); edge_pos++){
                    if (v.edges[edge_pos].edge_cap > 1e-10) break;
                }
                int height_to_reach = v.edges[edge_pos].neig_height + 1;
                // husky::base::log_msg("Vertex " + to_string(v.id()) + " propose to lift to height " + to_string(height_to_reach) + " based on its neighbour " + to_string(v.edges[edge_pos].neig_id) + " 's height " + to_string(v.edges[edge_pos].neig_height) + " with edge capacity " + to_string(v.edges[edge_pos].edge_cap));

                // broadcast the change to its neighbours with potential incoming edges
                for (edge_pos = 0; edge_pos < v.edges.size(); edge_pos++){
                    //if (v.edges[edge_pos].edge_cap < 3) -- it should not be a condition
                    // as neigbhours currently with incoming edges are exactly those become lower and thus later get push from this vertex
                    // and later when they got excess, they need to lift but they have no ways to know the updated height of this vertex
                    push_height_change_channel.push(HeightChange(v.id(), height_to_reach), v.edges[edge_pos].neig_id);
                    // husky::base::log_msg("proposing Vertex " + to_string(v.id()) + " to be lifted to height " + to_string(height_to_reach));
                }

                // one message to itself to keep the proposed height
                push_height_change_channel.push(HeightChange(v.id(), height_to_reach), v.id());
            }
        });

        

        // see if there are a conflict between adjacent vertices. If so, let the vertex with smaller id go first
        husky::list_execute(vertex_list, [&](VertexObj& v){
            vector<HeightChange> received_height_changes = push_height_change_channel.get(v);

            int proposed_height = 0;

            if ((v.height == record_max_to_lift_height) && (v.excess > 1e-10) && (v.id() != (vertex_total + 1)) && (v.id() != 0) && (received_height_changes.size() >= 1)){
                // if there is a height change proposed by a vertex with smaller id, then the proposal of this vertex has to drop
                int rhc_pos = 0;
                for (; rhc_pos < received_height_changes.size(); rhc_pos++){
                    if (received_height_changes[rhc_pos].vid < v.id()) {
                        husky::base::log_msg("Vertex " + to_string(v.id()) + " has a conflict with Vertex " + to_string(received_height_changes[rhc_pos].vid) + " during height lifting");
                        break; 
                    }
                    if (received_height_changes[rhc_pos].vid == v.id()) proposed_height = received_height_changes[rhc_pos].height;
                }

                // the change can be applied
                if (rhc_pos == received_height_changes.size()){
                    // lift the height
                    v.height = proposed_height;
                    for (int edge_pos = 0; edge_pos < v.edges.size(); edge_pos++){
                        //if (v.edges[edge_pos].edge_cap < 3)
                        // the condition is omitted: reason refer to the above loop
                        push_height_change_channel.push(HeightChange(v.id(), v.height), v.edges[edge_pos].neig_id);
                    }

                    // set push_flag to 1, and update excess_push_vertex_count to signfy new push procedure
                    v.push_flag = 1;
                    need_to_push = true;
                    husky::base::log_msg("Vertex " +  to_string(v.id()) + " has been lifted to Height " + to_string(proposed_height));
                }
            }
        });


        husky::list_execute(vertex_list, [&](VertexObj& v){
            vector<HeightChange> applied_height_changes = push_height_change_channel.get(v);

            for (auto ahc: applied_height_changes){
                for (int edge_pos = 0; edge_pos < v.edges.size(); edge_pos++){
                    if (v.edges[edge_pos].neig_id == ahc.vid){
                        v.edges[edge_pos].neig_height = ahc.height;
                    }
                }
            }


            // re-sort the edge list
            sort(v.edges.begin(), v.edges.end(), [](FlowEdge fe1, FlowEdge fe2){
                if (fe1.neig_height < fe2.neig_height) return true;
                else if (fe1.neig_height == fe2.neig_height) {
                    if (fe1.neig_id < fe2.neig_id) return true;
                    else return false;
                }

                return false;
            });
        });


        // check after lifting
        //*
        husky::list_execute(vertex_list, [&](VertexObj& v){
            husky::base::log_msg("Vertex " + to_string(v.id()) + " (Height " + to_string(v.height) + ") :");
            string info = "";
            // for (int i = 0; i < v.edges.size(); i++) {
            //     info += ("edge to vertex " + to_string(v.edges[i].neig_id) + "(height " + to_string(v.edges[i].neig_height) + ")," + " capacity: " + to_string(v.edges[i].edge_cap) + "\n");
            // }
            info += ("excess: " + to_string(v.excess) + " push_flag: " + to_string(v.push_flag) + "\n");
            husky::base::log_msg(info);
        });
        //*/

        husky::base::log_msg("***********************************************************************************************************************************************");
        
        iteration++;
    }

    auto& probe_channel = husky::ChannelStore::create_push_channel<int>(vertex_list, vertex_list);
    double source_sink_flow_sum = 0;
    set<int> dense_subgraph_nodes;

    // reaachable vertices propagte to reachable triangles
    husky::base::log_msg("Computation ends after Iteration " + to_string(iteration));
    husky::list_execute(vertex_list, [&](VertexObj& v){
        if (v.id() <= vertex_total){
            if (v.id() == 0) { 
                source_sink_flow_sum += v.excess; // backward, uncomsumable flow
            }
            // string info = "Vertex " + to_string(v.id()) + " towards Source with capacity ";
            for (int i = 0; i < v.edges.size(); i++){   
                if (v.edges[i].neig_id == 0){
                    // info += to_string(v.edges[i].edge_cap); husky::base::log_msg(info);

                    // ^_^ conditions for examining it indeeds has pushed back some flow to the source, i.e., reachable from the source in the residual network
                    // edge total = #incident triangles + 2 (to source and to sink)
                    if ((v.edges.size() - 2 - v.edges[i].edge_cap) > 1e-10){
                        for (auto e: v.edges){
                            if (e.edge_cap > 1e-10) probe_channel.push(1, e.neig_id);
                        }

                        // husky::base::log_msg("The dense subgraph includes Vertex " + to_string(v.id()));
                        dense_subgraph_nodes.insert(v.id());
                    }
                    break;
                }
            }
        }
    });

    // reachable triangles propagate to reachable vertices
    husky::list_execute(vertex_list, [&](VertexObj& v){
        vector<int> probe_from_vetex = probe_channel.get(v);
        if (v.id() == vertex_total + 1) {
                source_sink_flow_sum += v.excess; // maximum flow
                return;
            }
        if ((v.id() >= vertex_total + 2) && (probe_from_vetex.size() >= 1)){
            for (auto e:v.edges){
                if (e.edge_cap > 1e-10) probe_channel.push(1, e.neig_id);
            }
        }
    });

    // output reachable vertex by probing
    husky::list_execute(vertex_list, [&](VertexObj& v){
       vector<int> probe_from_tri = probe_channel.get(v);
        if ((v.id() <= vertex_total) && (probe_from_tri.size() >= 1)){
            // husky::base::log_msg("The dense subgraph includes Vertex " + to_string(v.id()));
            dense_subgraph_nodes.insert(v.id());
        }
    });

    if ((num_triangle.get_value() - source_sink_flow_sum) > 1e-10) {
        husky::base::log_msg("source_sink_flow_sum: " + to_string(source_sink_flow_sum));
        husky::base::log_msg("num_triangle: " + to_string(num_triangle.get_value()));
        husky::base::log_msg("********** Missing flow! ********** | ********** Missing flow! ********** | ********** Missing flow! ********** ");
    } else {
        husky::base::log_msg("Dense subgraph with the attempting triangle density: ");
        string included_node_list_str = "";
        for (auto node: dense_subgraph_nodes) included_node_list_str += (to_string(node) + " ");
        husky::base::log_msg(included_node_list_str);
    }

    husky::base::log_msg("Algorithm ends");

}

int main(int argc, char** argv) {
    // argv.push_back("iter");
    if (husky::init_with_args(argc, argv, {"hdfs_namenode", "hdfs_namenode_port", "input", "iter"})) {
        husky::run_job(triangle_listing);
        return 0;
    }
    return 1;
}
    