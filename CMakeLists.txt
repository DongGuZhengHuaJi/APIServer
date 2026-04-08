# cmake_minimum_required(VERSION 3.10)
# project(APIServer)

# set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_STANDARD_REQUIRED True)
# set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# # 1. 查找依赖
# find_package(PkgConfig REQUIRED)

# pkg_check_modules(HIREDIS REQUIRED hiredis)

# find_package(Boost REQUIRED COMPONENTS system thread)
# find_package(nlohmann_json REQUIRED)
# find_package(Threads REQUIRED)
# find_package(redis++ REQUIRED)

# # 2. 创建目标
# add_executable(api_server
#     api_server.cpp
#     logic_system.cpp
#     api_server.h
#     logic_system.h
#     thread_pool.hpp
#     redis_mgr.h
#     mysql_mgr.h
# )

# # 3. 使用 Target 方式链接（不再需要 include_directories）
# # 这种写法会自动处理包含路径和依赖关系，不会死循环
# target_link_libraries(api_server
#     PRIVATE
#     Boost::system
#     Boost::thread
#     nlohmann_json::nlohmann_json
#     Threads::Threads
#     hiredis::hiredis
#     redis++::redis++
#     ${HIREDIS_LIBRARIES}
# )

# target_include_directories(api_server
#     PRIVATE
#     ${HIREDIS_INCLUDE_DIRS}
# )

cmake_minimum_required(VERSION 3.10)
project(APIServer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ==========================================
# 1. 直接指定头文件搜索路径 (编译器找 .h)
# ==========================================
include_directories(
    "/usr/local/include"            # redis++ 的位置
    "/usr/include/hiredis"          # hiredis 的位置
    "/usr/include/mysqlconn"        # MySQL 的位置
    "${CMAKE_CURRENT_SOURCE_DIR}"   # 你自己的项目根目录
)

# ==========================================
# 2. 直接指定库文件的位置 (链接器找 .so)
# ==========================================
# 这里直接把库的绝对路径存到变量里
set(REDIS_PLUS_LIB "/usr/local/lib/libredis++.so")
set(HIREDIS_LIB     "/usr/lib/x86_64-linux-gnu/libhiredis.so")
set(MYSQL_LIB       "/usr/lib/x86_64-linux-gnu/libmysqlcppconn.so")

# ==========================================
# 3. 查找 Boost 和线程 (这两个通常 apt 装了后 find_package 很稳)
# ==========================================
find_package(Boost REQUIRED COMPONENTS system thread)
find_package(Threads REQUIRED)

# ==========================================
# 4. 定义你的程序文件
# ==========================================
add_executable(api_server
    api_server.cpp
    logic_system.cpp
    mysql_pool.h
    api_server.h
    logic_system.h
    thread_pool.hpp
    redis_mgr.h
    mysql_mgr.h
    mysql_pool.h

    # .h 和 .hpp 文件编译器会自动找，不需要写在这里
)

# ==========================================
# 5. 最后一把抓：链接所有库
# ==========================================
target_link_libraries(api_server
    PRIVATE
    ${REDIS_PLUS_LIB}
    ${HIREDIS_LIB}
    ${MYSQL_LIB}
    Boost::system
    Boost::thread
    Threads::Threads
)