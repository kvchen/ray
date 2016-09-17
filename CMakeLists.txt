cmake_minimum_required(VERSION 2.8)

project(ray)

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

if(NOT APPLE)
    find_package(PythonInterp REQUIRED)
    find_package(PythonLibs REQUIRED)
    set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
else()
    find_program(CUSTOM_PYTHON_EXECUTABLE python)
    message("-- Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print 'python' + sys.version[0:3]"
                    OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print sys.exec_prefix"
                    OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
    FIND_LIBRARY(PYTHON_LIBRARIES
                 NAMES ${PYTHON_LIBRARY_NAME}
                 HINTS "${PYTHON_PREFIX}"
                 PATH_SUFFIXES "lib" "libs"
                 NO_DEFAULT_PATH)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "from distutils.sysconfig import *; print get_python_inc()"
                    OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(PYTHON_LIBRARIES AND PYTHON_INCLUDE_DIRS)
        SET(PYTHONLIBS_FOUND TRUE)
        message("-- Found PythonLibs: " ${PYTHON_LIBRARIES})
        message("-- -- Used custom search path")
    else()
        find_package(PythonLibs REQUIRED)
        message("-- -- Used find_package(PythonLibs)")
    endif()
endif()

find_package(NumPy REQUIRED)
find_package(Boost REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/")
include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")
include_directories("/usr/local/include")
include_directories("${Boost_INCLUDE_DIRS}")

link_libraries(${CMAKE_SOURCE_DIR}/thirdparty/hiredis/libhiredis.a
               pthread)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

set(ARROW_LIB ${CMAKE_SOURCE_DIR}/thirdparty/arrow-old/cpp/build/release/libarrow.a)

add_definitions(-fPIC)
