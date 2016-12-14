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

class Vertex {
   public:
    using KeyT = int;
    KeyT object_id;
    KeyT node_id;
    const KeyT& id() const { return object_id; }

    vector<int> neighbours;
    vector<int> removable_neighbours; // record the removable neighbours (already been used for trianle listing)

    Vertex(){};
    // explicit Vertex(const KeyT& init_node_id, vector<pair<int, int> > edge_list): object_id(init_node_id),
    // node_id(init_node_id){

    // }
    explicit Vertex(const KeyT& init_node_id) : object_id(init_node_id), node_id(init_node_id) {}

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.object_id << v.node_id << v.neighbours << v.removable_neighbours;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.object_id >> v.node_id >> v.neighbours >> v.removable_neighbours;
        return stream;
    }
};

class Block {
   public:
    using KeyT = int;
    KeyT block_id;
    const KeyT& id() const { return block_id; }

    vector<Vertex> subgraph;

    Block(){};
    // explicit Block(const KeyT& init_block_id, vector<Vertex> vertice_list): block_id(init_block_id){
    //  for (int iter = 0; iter < vertice_list.size(); iter++)
    //      { subgraph.push_back(vertice_list[iter]); }
    // }

    explicit Block(const KeyT& init_block_id) : block_id(init_block_id) {}

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Block& block) {
        stream << block.block_id << block.subgraph;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Block& block) {
        stream >> block.block_id >> block.subgraph;
        return stream;
    }

    int merge_rest_subgraph(vector<Vertex> rest_subgraph) {
        for (int iter = 0; iter < rest_subgraph.size(); iter++) {
            subgraph.push_back(rest_subgraph[iter]);
        }
        return 0;
    }

    int merge_block(Block next_block) {
        for (int i = 0; i < next_block.subgraph.size(); i++) {
            subgraph.push_back(next_block.subgraph[i]);
        }
    }
};

struct Triangle {
    int x;
    int y;
    int z;
    Triangle() = default;
    Triangle(int x, int y, int z) : x(x), y(y), z(z) {};
};

struct Edge {
    int vert_a;
    int vert_b;
    Edge() = default;
    Edge(int a, int b) : vert_a(a), vert_b(b) {};
};

/*
class TriangleSet {
   public:
    using KeyT = int;
    KeyT TriangleSet_id;
    const KeyT& id() const { return TriangleSet_id; }

    vector<Triangle> triangle_list;

    TriangleSet(){};

    explicit TriangleSet(const KeyT& init_trianleset_id) : TriangleSet_id(init_trianleset_id) {}
};
*/

class TriangleObj {
public:
    using KeyT = int;
    KeyT triangle_id;
    const KeyT& id() const { return triangle_id; }

    int vert_a;
    int vert_b;
    int vert_c;

    TriangleObj(){};
    explicit TriangleObj(const KeyT& init_triangle_id) : triangle_id(init_triangle_id) {}
    void buildTriangleObj(int& a, int& b, int& c) { vert_a = a; vert_b = b; vert_c = c; }
};

void triangle_listing() {
    husky::io::LineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));
    float subgraph_size = 2.0;

    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>();
   
    //auto& triangle_set_list = husky::ObjListStore::create_objlist<TriangleSet>();

    int vertex_total = 0;

    auto parse_graph_input = [&vertex_list, &vertex_total](boost::string_ref& neighbour_list) {
        if (neighbour_list.size() == 0)
            return;

        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(neighbour_list, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator iter = tok.begin();

        int node_id = stoi(*iter++);
        Vertex v(node_id);
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
    husky::list_execute(vertex_list, [&](Vertex& vertex) {

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
                }
            }
        }

    });


    // *************** matching triangles ***************

    // use Aggregator to get the sum of triangles TIMELY, and assign id to them
    husky::lib::Aggregator<int> triangle_num(0, [](int& a, const int& b){ a += b; });
    auto& ac = husky::lib::AggregatorFactory::get_channel();

    auto& triangle_list = husky::ObjListStore::create_objlist<TriangleObj>();
    auto& push_triangle_channel = husky::ChannelStore::create_push_channel<Triangle>(vertex_list, triangle_list);
    husky::globalize(triangle_list);

    // match the edges and push the triangles to a new list
    vector<husky::ChannelBase*>chs = {&ac, &push_triangle_channel};
    husky::list_execute(vertex_list, {&edge_push_channel}, chs, [&](Vertex& vertex){
        int thread_ctr = 0;
        int thread_id = husky::Context::get_global_tid();
        int thread_total = husky::Context::get_num_global_workers();
        vector<Edge> received_edges = edge_push_channel.get(vertex);
        for (int edge_ctr = 0; edge_ctr < received_edges.size(); edge_ctr++){
            // checking if the second vertex is a neighbour
            // can be replaced with a binary search
            int vert_b_id = received_edges[edge_ctr].vert_b;
            for (int neig_ctr = 0; neig_ctr < vertex.neighbours.size(); neig_ctr++){
                if (vertex.neighbours[neig_ctr] == vert_b_id){
                    cout << "Triangle (" << received_edges[edge_ctr].vert_a << " " << vert_b_id << " " << vertex.id() << ") found\n";
                    triangle_num.update(1);
                    //husky::lib::AggregatorFactory::sync();
                    //cout << "trianle_num updated to " << triangle_num.get_value() << endl;
                    //push_triangle_channel.push(Triangle(received_edges[edge_ctr].vert_a, vert_b_id, vertex.id()), triangle_num.get_value());
                    push_triangle_channel.push(Triangle(received_edges[edge_ctr].vert_a, vert_b_id, vertex.id()), thread_ctr * thread_total + thread_id);
                    thread_ctr++;
                    // cout << "should have pushed to TriangleObj " << triangle_num.get_value() << endl;
                }
            }
        }
    });

    cout << "trianle_num updated to " << triangle_num.get_value() << endl;

    // BUILD and print out the triangles for examination of their ids (generation order)
    husky::list_execute(triangle_list, [&](TriangleObj& triobj){
        vector<Triangle> received_tri = push_triangle_channel.get(triobj);
        
        if (received_tri.size() > 1) cout << "triangle id duplicate !!!!!\n";
        
        // triobj.buildTriangleObj(received_tri[0].x, received_tri[0].y, received_tri[0].z);
        // cout << "TriangleObj with id " << triobj.id() << " and vertices (" << triobj.vert_a << ", " << triobj.vert_b << ", " << triobj.vert_c << ")\n";
        

        for (auto rt: received_tri) { cout << "TriObj with id " << triobj.id() << " Received: (" << rt.x << " " << rt.y << " " << rt.z << ")\n"; }
    });

    //*/

    // logging the triangles
    /*
       husky::list_execute(triangle_set_list, [&](TriangleSet& ts){
       cout << "From the vertex " << ts.id() << endl;
       vector<Triangle> tri_list = counting_channel.get(ts);
       for (int i = 0; i < tri_list.size(); i++){
       husky::base::log_msg((to_string(std::get<0>(tri_list[i])) + to_string(std::get<1>(tri_list[i])) +
    to_string(std::get<2>(tri_list[i]))));
       }
       });
    //*/

    /*
    // first print
    cout << "The First way to print next_block_list\n";
    husky::list_execute(block_list, [&](Block& block) {
        cout << "In thread " << husky::Context::get_global_tid() << " and Block id: " << block.id() << endl;
        cout << "vertice and edges involved: ";
        for (int i = 0; i < block.subgraph.size(); i++) {
            cout << "Vetex " << block.subgraph[i].id() << "\nneighbours: ";
            for (int j = 0; j < block.subgraph[i].neighbours.size(); j++){
                cout << block.subgraph[i].neighbours[j] << " ";
            }
            cout << endl;
            cout << "removable neighbours: ";
           //for (int j = 0; j < block.subgraph[i].removable_neighbours.size(); j++){
           //    cout << block.subgraph[i].removable_neighbours[j] << " ";
           //}
           for (auto rmv_neg: block.subgraph[i].removable_neighbours) cout << rmv_neg << " ";
           cout << endl;
        }
        cout << endl;
    });
    //*/


}

int main(int argc, char** argv) {
    if (husky::init_with_args(argc, argv, {"hdfs_namenode", "hdfs_namenode_port", "input"})) {
        husky::run_job(triangle_listing);
        return 0;
    }
    return 1;
}
