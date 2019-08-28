#include <fstream>

int main(int argc, char** argv)
{
    std::ofstream fp("/proc/sys/vm/drop_caches");
    fp << "3";
    return 0;
}
