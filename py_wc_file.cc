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
  dataset A = mr.make_dataset({argv[1]});

  A.pymap("def map(line):"
      "\n\td = { }"
      "\n\tfor i in line.split():"
      "\n\t\tif i in d:"
      "\n\t\t\td[i].append('1')"
      "\n\t\telse:"
      "\n\t\t\td[i] = ['1']"
      "\n\treturn d\n"
      );
  A.pyreduce("def reduce(a,b):"
      "\n\treturn str(int(a)+int(b))\n"
      , "output");

}
