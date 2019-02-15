#include <libmemcached/memcached.h>
#include <eclipsedfs/vmr.hh>
#include <eclipsedfs/output_collection.hh>
#include <utility>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <list>
#include <unordered_map>
#include <limits>
#include <math.h>
#include <string.h>
#include <time.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include <vector>


#define INPUT_NAME "kmeans_1G.input"
//#define INPUT_NAME "2block.input"
#define ITERATIONS 5 
#define NUM_CLUSTERS 9 
#define NUM_SERVERS 9//7 //10

using namespace std;
using namespace velox;

extern "C" {
  void before_map(std::unordered_map<std::string, void*>&);
  void after_map(std::unordered_map<std::string, void*>&);
  void mymapper(std::string&, velox::OutputCollection&, std::unordered_map<std::string, void*>&);
  void mymapper2nd(std::string&, int, velox::OutputCollection&, velox::OutputCollection&, std::unordered_map<std::string, void*>&);
  void mymapper3rd(std::vector<std::string>&, velox::OutputCollection&, std::unordered_map<std::string, int>&);
  void myreducer(std::string&, std::vector<std::string>&);
  void myreducer2nd(std::string&, std::vector<std::string>&);
}

class Point {
  private:
    double x;
    double y;
    int num;
    
  public:
    Point() = default;
    Point(double _x, double _y) : x(_x), y(_y) {};
    Point(std::string& str) {
      get_point_from_string(str);
    };
    Point(std::string& str, int n) {
      red_get_point_from_string(str);
    };
    Point(const Point& other) {
      x = other.x;
      y = other.y;
    };

    Point& operator=(const Point& other) {
      x = other.x;
      y = other.y;

      return *this;
    };

    double getX() {
      return x;
    };
    
    double getY() {
      return y;
    };

    int getNum() {
      return num;
    };

    void setX(double _x) {
      x = _x;
    };

    void setY(double _y) {
      y = _y;
    };

    void get_point_from_string(std::string str) {
      sscanf(str.c_str(), "%lf,%lf", &x, &y);
    };

    void red_get_point_from_string(std::string str) {
      sscanf(str.c_str(), "%lf,%lf,%d", &x, &y, &num);
    };
    
    double distance_square(Point& p) {
      return (double)(pow((x - p.x), 2.0) + pow((y - p.y), 2.0));
    };

    static double distance_square(Point& p1, Point& p2) {
      return (pow((p1.x - p2.x), 2.0) + pow((p1.y - p2.y), 2.0));
    };

    static double distance(Point& p1, Point& p2) {
      return distance_square(p1, p2);
    };

    std::string to_string() {
      char tmp [512];
      string to_send;
      to_send.reserve(512);

      size_t total = snprintf(tmp, 512, "%lf,%lf", x, y);
      to_send.append(tmp,total);


      return std::move(to_send);
    };
};

static string group_keys[NUM_SERVERS] = {"dicl", "peach", "no", "dfs", "map", "hadoop", "pear", "memc", "apple"};
inline memcached_st *_get_memcached() {
    const char *server = "--SERVER=dumbo051 --SERVER=dumbo052 --SERVER=dumbo053 --SERVER=dumbo054 --SERVER=dumbo055 --SERVER=dumbo056 --SERVER=dumbo057 --SERVER=dumbo058 --SERVER=dumbo059";
    return memcached(server, strlen(server));
}

void before_map(std::unordered_map<std::string, void*>& options) {
    //soojeong
    size_t value_length;
    memcached_return_t rt;
    memcached_st *memc2 = _get_memcached();
    uint32_t flags;
    //

    std::list<Point>* centroids = new std::list<Point>();
    
    //soojeong
    for(int i=0; i<NUM_CLUSTERS; ++i) {
        string key = "prev_" + to_string(i);
        int j = i % NUM_SERVERS;
        char *value = memcached_get_by_key(memc2, 
                group_keys[j].c_str(), group_keys[j].size(),
                key.c_str(), key.size(), 
                &value_length, &flags, &rt);

        if(value) {
            string centroid_str(value, value_length);
            Point centroid(centroid_str);
            centroids->push_back(std::move(centroid));
        }
        else {
            string test_centroid = "0.0, 0.0";
            rt = memcached_set_by_key(memc2, group_keys[j].c_str(), group_keys[j].size(), test_centroid.c_str(), test_centroid.size(), test_centroid.c_str(), test_centroid.size(), (time_t)0, (uint32_t)0);  
            Point centroid(test_centroid);
            centroids->push_back(std::move(centroid));
        } 
    }   

    memcached_free(memc2);
    options["centroids"] = centroids;

}

void after_map(std::unordered_map<std::string, void*>& options) {
  for(auto it = options.begin(); it != options.end(); ++it) 
    delete reinterpret_cast<std::list<Point>*>(it->second);
}

void mymapper(std::string& input, velox::OutputCollection& mapper_results, std::unordered_map<std::string, void*>& options) {
  std::list<Point>* centroids = reinterpret_cast<std::list<Point>*>(options["centroids"]);
  
  stringstream ss(input);
  string pstring;

  while( ss >> pstring){ 
      Point p(pstring);

      double min = std::numeric_limits<double>::max();
      Point nearest_centroid;

      for(Point centroid : *centroids) {
          double dist = Point::distance_square(p, centroid);
          if(dist < min) {
              nearest_centroid = centroid;
              min = dist;
          }
      }
      mapper_results.insert(nearest_centroid.to_string(), p.to_string());
  }

}

void mymapper2nd(std::string& input, int now_server, velox::OutputCollection& mapper_add, velox::OutputCollection& mapper_del, std::unordered_map<std::string, void*>& options) {
  std::list<Point>* centroids = reinterpret_cast<std::list<Point>*>(options["centroids"]);
    
  Point p(input);

//--soojeong------
//get now centroid from prev_number
  list<Point>::iterator iter = centroids->begin();
  std::advance(iter, now_server);
  Point origin_centroid = *iter;
  int count = 0;
  int new_server = 0;
//-----------

  double min = std::numeric_limits<double>::max();
  Point nearest_centroid;
  for(Point centroid : *centroids) {
    double dist = Point::distance_square(p, centroid);
    if(dist < min) {
      nearest_centroid = centroid;
      min = dist;
      new_server = count;
    }
    count++;
  }
//----------------
  if((nearest_centroid.to_string()).compare(origin_centroid.to_string()) != 0){
    string del = to_string(now_server);
    string add = to_string(new_server);
    mapper_del.insert(del, p.to_string());
    mapper_add.insert(add, p.to_string());
  }
}

void mymapper3rd(std::vector<std::string>& input, velox::OutputCollection& mapper_results, std::unordered_map<std::string, int>& cents){
  struct timeval s, e;
  float total = 0.0;
  memcached_return_t rt;
  memcached_st *memc3 = _get_memcached();

  for(auto& in : input){
  
      Point p(in);

      double min = std::numeric_limits<double>::max();
      Point nearest_centroid;

      //for(Point centroid : *centroids) {
      //I think here is the bottleneck!
      for(auto& c : cents) {
      gettimeofday(&s, NULL);
          std::string cc = c.first;
          Point centroid(cc);
      gettimeofday(&e, NULL);
      total += (e.tv_sec - s.tv_sec);
          double dist = Point::distance_square(p, centroid);
          if(dist < min) {
              nearest_centroid = centroid;
              min = dist;
          }
      }
      mapper_results.insert(nearest_centroid.to_string(), p.to_string());
  }
  rt = memcached_set(memc3, to_string(total).c_str(), to_string(total).size(), to_string(total).c_str(), to_string(total).size(), (time_t)0, (uint32_t)0); 
  if(!rt) { }
}

void myreducer(std::string& key, std::vector<std::string>& values) {
//soojeong--open memcached servers----------
  memcached_return_t rt;
  memcached_st *memc3 = _get_memcached();
  size_t value_length;
  uint32_t flags;
//  std::unordered_map<std::string, void*> map_value;

//---calculate new centroids-----------
 if(values.size() == 0) return;

  double sumX = 0, sumY = 0;
  unsigned int count = 0;
  std::string value_string = "";
  for(std::string& current_value : values) {
    Point p(current_value, 0); 
  
    sumX += p.getX() * p.getNum();
    sumY += p.getY() * p.getNum();
    count+= p.getNum();
   
 //   value_string += p.to_string();
 //   if(count < values.size()) value_string += " ";
  }

  Point centroid((sumX / count), (sumY / count));
//---store all datas in memcached----------------
  char* prev;
  for(int i =0; i < NUM_CLUSTERS ; i++){
    string c_n2 = "prev_" + to_string(i);
    string c = "1st_"+to_string(i);
    int j = i % NUM_SERVERS;
    prev = memcached_get_by_key(memc3, group_keys[j].c_str(), 
                                group_keys[j].size(),
                                c_n2.c_str(), c_n2.size(),
                                &value_length, &flags, &rt); 
    if(prev == key){
        rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), c_n2.c_str(), c_n2.size(), centroid.to_string().c_str(), centroid.to_string().size(), (time_t)0, (uint32_t)0);
        if(rt != MEMCACHED_SUCCESS){
        }     
        rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), c.c_str(), c.size(), to_string(count).c_str(), to_string(count).size(), (time_t)0, (uint32_t)0);  
        break;
    }   
  }   
//
 
}

void myreducer2nd(std::string& key, std::vector<std::string>& values) {
//soojeong--open memcached servers----------
  memcached_return_t rt;
  memcached_st *memc3 = _get_memcached();
//  size_t value_length;
//  uint32_t flags;

  if(values.size() == 0) return;
  int j = stoi(key) % NUM_SERVERS;
/*
//read memcached_data
  string skey = "cent_" + key;
  char* origin = memcached_get_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), skey.c_str(), skey.size(), &value_length, &flags, &rt); 
  
  uint64_t temp = strtoull(origin, NULL, 0);
  std::unordered_multimap<std::string, void*>* prePoints = reinterpret_cast<unordered_multimap<std::string, void*>*>(temp);

//---calculate new centroids-----------

  double sumX = 0, sumY = 0;//, sumdX=0, sumdY=0;
  unsigned int count = 0;
  int delcount = 0;
  int addcount = 0;
  std::string value_string = "";

  bool isAdd = false;
  int tot = 0;
  for(std::string& current_value : values) {
    tot++;
    if(current_value == "add"){ isAdd=true; continue; }
    if(current_value == "del"){ isAdd=false; continue; }
    if(isAdd){
      addcount++;
      prePoints->insert({current_value, NULL});
    }else{
      prePoints->erase(current_value);
      delcount++;
    }
  }
*/
  double sumX = 0, sumY = 0;
  unsigned int count = 0;
  std::string value_string = "";
  for(std::string& current_value : values) {
    Point p(current_value, 0); 
  
    sumX += p.getX() * p.getNum();
    sumY += p.getY() * p.getNum();
    count+= p.getNum();
   
 //   value_string += p.to_string();
 //   if(count < values.size()) value_string += " ";
  }

  Point centroid((sumX / count), (sumY / count));

//---store all datas in memcached----------------
  string c_n2 = "prev_" + key;
//  string dd = "2nd_d_" + key;
//  string dd = "2nd_d_" + to_string(delcount);
//  string aa = "2nd_a_" + to_string(addcount);
//  string aa = "2nd_a_" + key;
//  rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), to_string(count).c_str(), to_string(count).size(), centroid.to_string().c_str(), centroid.to_string().size(), (time_t)0, (uint32_t)0);
//  rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), dd.c_str(), dd.size(), to_string(delcount).c_str(), to_string(delcount).size(), (time_t)0, (uint32_t)0);
//  rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), aa.c_str(), aa.size(), to_string(addcount).c_str(), to_string(addcount).size(), (time_t)0, (uint32_t)0);
  rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), c_n2.c_str(), c_n2.size(), centroid.to_string().c_str(), centroid.to_string().size(), (time_t)0, (uint32_t)0);
//  rt = memcached_set_by_key(memc3, group_keys[j].c_str(), group_keys[j].size(), to_string(tot).c_str(), to_string(tot).size(), to_string(tot).c_str(), to_string(tot).size(), (time_t)0, (uint32_t)0);  
  if(rt != MEMCACHED_SUCCESS){ }

//
}

int main (int argc, char** argv) {
  vdfs cloud;

//temp: make a file for centroid in nfs, need to fix with memcache key-value cache
  memcached_return_t rt;
  memcached_st *memc = _get_memcached();

  string key;
  long kmeans;
  struct timeval tmp, getIng, getmap, getreduce;
  // initialize the first centroid points randomly
  std::cout << "VARIABLES" << std::endl;
  std::cout << "# of iterations: " << ITERATIONS << std::endl;
  std::cout << "# of clusters(k): " << NUM_CLUSTERS << std::endl;

  std::cout << "Initial Centroids: " << std::endl;
  gettimeofday(&getIng, NULL);
  srand(time(nullptr));
  double x[9] = {87.490000, 46.210000, 49.000000, 73.050000, 94.650000, 79.130000, 38.650000, 83.050000, 12.345000};
  double y[9] = {37.160000, 93.590000, 96.440000, 42.670000, 6.140000, 53.680000, 38.850000, 27.860000, 67.890000};
  for(int i=0; i<NUM_CLUSTERS; i++) {
//    double x =((double)(rand()%10001)/100);
//    double y =((double)(rand()%10001)/100);
//    Point centroid(x, y);
    Point centroid(x[i], y[i]);
    std::string centroid_content = centroid.to_string();
    key = "prev_" + to_string(i);
    //soojeong: first i store all cluters centroid at dumbo030
    int j = i % NUM_SERVERS;
    rt = memcached_set_by_key(memc, group_keys[j].c_str(), group_keys[j].size(), key.c_str(), key.size(), centroid_content.c_str(), centroid_content.size(), (time_t)0, (uint32_t)0);
    if(rt!=MEMCACHED_SUCCESS) {
        cout << " nonononono " << endl;
        cout << rt << endl;
    }
    std::cout << centroid_content << endl;
  }
  std::cout << "========================" << std::endl;

  long map; //, reduce, kmeans;
  vmr mr(&cloud);
  for(int i=0; i<ITERATIONS; i++) {
    std::cout << "MR iteration " << i << std::endl;

    dataset A = mr.make_dataset({INPUT_NAME});
  //  if(i == 0){   
      gettimeofday(&getmap, NULL);
      A.map("mymapper3rd");
      gettimeofday(&getreduce, NULL);
      map =getreduce.tv_sec - getmap.tv_sec;
      cout << map << " " << getreduce.tv_sec << " " << getmap.tv_sec << endl;
      A.reduce("myreducer");
      gettimeofday(&tmp, NULL);
      //reduce = tmp.tv_sec - getreduce.tv_sec;
    //  cout << reduce << " " << tmp.tv_sec << " " << getreduce.tv_sec << endl;
   // }
   /*
   else{
      gettimeofday(&getmap, NULL);
      A.map("mymapper2nd");
      gettimeofday(&getreduce, NULL);
      A.reduce("myreducer2nd");
      gettimeofday(&tmp, NULL);
      map =getreduce.tv_sec - getmap.tv_sec;
      cout << map << " " << getreduce.tv_sec << " " << getmap.tv_sec << endl;
      //reduce = tmp.tv_sec - getreduce.tv_sec;
      //cout << reduce << " " << tmp.tv_sec << " " << getreduce.tv_sec << endl;
    }*/ 
  }

  gettimeofday(&tmp, NULL);
  kmeans = tmp.tv_sec - getIng.tv_sec;
  cout << kmeans << " " << tmp.tv_sec << " " << getIng.tv_sec << endl;
  cout << "[KMEANS Time]" << kmeans << "sec"  << endl;


  std::cout << "FINISH k-means clusering" << std::endl;
  // summary
  std::cout << "<<File_name: " << INPUT_NAME << ", iter: " << ITERATIONS << " >>" << endl;
  std::cout << "========================" << std::endl;
  std::cout << "Centroids" << std::endl;
  

   //get value from key
  size_t value_length;
  uint32_t flags;

  for(int i=0; i<NUM_CLUSTERS; i++){
    string c_n = "prev_" + to_string(i);
    int j = i % NUM_SERVERS;
    char *value = memcached_get_by_key(memc, group_keys[j].c_str(), group_keys[j].size(), c_n.c_str(), c_n.size(), &value_length, &flags, &rt);
    if(value){
        cout << "[cent" << i << "]: "<< value << endl;
    }
  }
  memcached_free(memc);
  std::cout << "Total # of centroids:" << NUM_CLUSTERS << std::endl;
  
  return 0;
}
