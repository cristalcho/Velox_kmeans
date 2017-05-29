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
#include <fcntl.h>
#include <unistd.h>


#define INPUT_NAME "kmeans100mb.input"
#define OUTPUT_NAME "kmeans.output"
#define CENTROID_NAME "kmeans_centroids.data"
#define LOCAL_CENTROID_PATH "/home/vicente/velox_test/kmeans_centroids.data"
#define LOCAL_CENTROID_PATH_LOCK "/home/vicente/velox_test/kmeans_centroids.data.lock"
#define ITERATIONS 5
#define NUM_CLUSTERS 25

using namespace std;
using namespace velox;

extern "C" {
  void before_map(std::unordered_map<std::string, void*>&);
  void after_map(std::unordered_map<std::string, void*>&);
  void mymapper(std::string&, velox::OutputCollection&, std::unordered_map<std::string, void*>&);
  void myreducer(std::string&, std::vector<std::string>&, OutputCollection&);
}

class Point {
  private:
    double x;
    double y;

  public:
    Point() = default;
    Point(double _x, double _y) : x(_x), y(_y) {};
    Point(std::string& str) {
      get_point_from_string(str);
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

    void setX(double _x) {
      x = _x;
    };

    void setY(double _y) {
      y = _y;
    };

    void get_point_from_string(std::string str) {
      //std::replace( str.begin(), str.end(), ',', ' ');
      //char* end;
      //x = strtod(str.c_str(), &end);
      //y = strtod(str.c_str(), &end);
      sscanf(str.c_str(), "%lf,%lf", &x, &y);
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
      //return (std::to_string(x) + "," + std::to_string())
      char tmp [512];
      string to_send;
      to_send.reserve(512);

      size_t total = snprintf(tmp, 512, "%lf,%lf", x, y);
      to_send.append(tmp,total);


      return std::move(to_send);
    };
};

void before_map(std::unordered_map<std::string, void*>& options) {
  ifstream fs;
  fs.open(LOCAL_CENTROID_PATH);

  std::list<Point>* centroids = new std::list<Point>();

  std::string centroid_str;
  while(getline(fs, centroid_str)) {
    Point centroid(centroid_str);
    centroids->push_back(std::move(centroid));
  }

  fs.close();

  options["centroids"] = centroids;
}

void after_map(std::unordered_map<std::string, void*>& options) {
  for(auto it = options.begin(); it != options.end(); ++it) 
    delete reinterpret_cast<std::list<Point>*>(it->second);
}

void mymapper(std::string& input, velox::OutputCollection& mapper_results, std::unordered_map<std::string, void*>& options) {
  std::list<Point>* centroids = reinterpret_cast<std::list<Point>*>(options["centroids"]);

  Point p(input);

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

void myreducer(std::string& key, std::vector<std::string>& values, OutputCollection& output) {
  if(values.size() == 0) return;

  double sumX = 0, sumY = 0;
  unsigned int count = 0;
  std::string value_string = "";
  for(std::string& current_value : values) {
    Point p(current_value); 
  
    sumX += p.getX();
    sumY += p.getY();

    count++;

    value_string += p.to_string();
    if(count < values.size()) value_string += " | ";
  }

  Point centroid((sumX / count), (sumY / count));
  output.insert(centroid.to_string(), value_string);


  {
    struct flock lock;
    int fd = open (LOCAL_CENTROID_PATH_LOCK, O_WRONLY|O_CREAT, 0644);
    /* Initialize the flock structure. */
    memset (&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;
    /* Place a write lock on the file. */
    while (-1 == fcntl (fd, F_SETLKW, &lock) );

    std::ofstream os;
    os.open(LOCAL_CENTROID_PATH, ios::app);
    os << centroid.to_string() << endl;
    os.close();

    /* Release the lock. */
    lock.l_type = F_UNLCK;
    fcntl (fd, F_SETLKW, &lock);

    close (fd);
  }

}

int main (int argc, char** argv) {
  vdfs cloud;

  std::remove(LOCAL_CENTROID_PATH);
  // temp: make a file for centroid in nfs
  std::ofstream os;
  os.open(LOCAL_CENTROID_PATH);
  if(!os.is_open()) return 1;

  // initialize the first centroid points randomly
  std::cout << "VARIABLES" << std::endl;
  std::cout << "# of iterations: " << ITERATIONS << std::endl;
  std::cout << "# of clusters(k): " << NUM_CLUSTERS << std::endl;

  std::cout << "Initial Centroids: " << std::endl;
  srand(time(nullptr));
  for(int i=0; i<NUM_CLUSTERS; i++) {
    double x = ((double)(rand() % 10001) / 100);
    double y = ((double)(rand() % 10001) / 100);
    Point centroid(x, y);
    std::string centroid_content = centroid.to_string() + "\n";
    std::cout << centroid_content;
    os.write(centroid_content.c_str(), centroid_content.size());
  }
  os.close();

  std::cout << "========================" << std::endl;

  vmr mr(&cloud);

  std::string output_name;
  for(int i=0; i<ITERATIONS; i++) {
    std::cout << "MR iteration " << i << std::endl;

    dataset A = mr.make_dataset({INPUT_NAME});

    output_name = "kmeans.output-" + to_string(i);

    A.map("mymapper");
    std::remove(LOCAL_CENTROID_PATH);
    A.reduce("myreducer", output_name);
  }


  std::cout << "FINISH k-means clusering" << std::endl;

  // summary
  std::cout << "========================" << std::endl;
  std::cout << "Centroids" << std::endl;

  ifstream fs;
  fs.open(LOCAL_CENTROID_PATH);
  std::string output_line;
  int cnt = 0;
  while(getline(fs, output_line)) {
    cout << output_line << endl; 
    cnt++;
  }

  std::cout << "Total # of centroids:" << cnt << std::endl;

  return 0;
}
