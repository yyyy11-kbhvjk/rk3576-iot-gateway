cmake_minimum_required(VERSION 3.16)

project(rk3576_iot_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_check_modules(
    MOSQUITTO
    REQUIRED
    IMPORTED_TARGET
    libmosquitto
)

add_executable(gateway
    src/main.cpp
    src/database/Database.cpp
    src/mqtt/MqttClient.cpp
)

target_link_libraries(gateway
    PRIVATE
    SQLite::SQLite3
    PkgConfig::MOSQUITTO
)

target_compile_options(gateway
    PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
