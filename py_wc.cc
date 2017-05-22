#include <eclipsedfs/vmr.hh>
#include <utility>
#include <string>
#include <sstream>
#include <iostream>

using namespace velox;
using namespace std;

int main(int argc, char** argv) {
  vdfs cloud;
  vmr mr(&cloud);

  file file_a = cloud.open("sample_file");

  file_a.append(
      "HELLO\n"
      "HELLO\n"
      "BYE\n"
      "BYE\n"
      /*... */
      "HOLA\n"
      "HOLA\n"
      "BYE\n"
      "HOLA\n"
      "BYE\n"
      "ADIOS\n");

  cout << file_a.get() << endl;

  dataset A = mr.make_dataset({"sample_file"});
  A.pymap("def map(line):"
      "\n\td = { }"
      "\n\taword = line.split()[0]"
      "\n\td[aword] = '1'"
      "\n\treturn d\n"
      );
  A.pyreduce("def reduce(a,b):"
      "\n\treturn str(int(a)+int(b))\n"
      , "output");

  cloud.rm("sample_file");

  // Print out the final result
  cout << cloud.open("output").get() << endl; 

  //cloud.format(); // Optional
}
