
cmake_minimum_required ( VERSION 2.8.12 )

# toolchain setup
if ( NOT DEFINED STATICLIB_DEPS )
    set ( STATICLIB_DEPS ${CMAKE_CURRENT_LIST_DIR}/../../../staticlibs CACHE INTERNAL "" )    
endif ( )
set ( STATICLIB_TOOLCHAIN linux_amd64_gcc CACHE STRING "toolchain triplet" )
if ( NOT DEFINED STATICLIB_CMAKE )
    set ( STATICLIB_CMAKE ${STATICLIB_DEPS}/cmake CACHE INTERNAL "" )    
endif ( )
set ( CMAKE_TOOLCHAIN_FILE ${STATICLIB_CMAKE}/toolchains/${STATICLIB_TOOLCHAIN}.cmake CACHE INTERNAL "" )

# project
project ( medprint CXX )
include ( ${STATICLIB_CMAKE}/staticlibs_common.cmake )
staticlib_enable_deplibs_cache ( )

# dependencies
staticlib_add_subdirectory ( ${CMAKE_CURRENT_LIST_DIR}/../../wilton_core )
staticlib_add_subdirectory ( ${CMAKE_CURRENT_LIST_DIR}/../../wilton_db )
set ( ${PROJECT_NAME}_DEPS
        wilton_core
        wilton_db )
staticlib_pkg_check_modules ( ${PROJECT_NAME}_DEPS_PC REQUIRED ${PROJECT_NAME}_DEPS )

# tests
set ( ${PROJECT_NAME}_TEST_INCLUDES ${${PROJECT_NAME}_DEPS_PC_INCLUDE_DIRS} )
set ( ${PROJECT_NAME}_TEST_LIBS ${${PROJECT_NAME}_DEPS_PC_LIBRARIES} )
set ( ${PROJECT_NAME}_TEST_OPTS ${${PROJECT_NAME}_DEPS_PC_CFLAGS_OTHER} )
staticlib_enable_testing ( ${PROJECT_NAME}_TEST_INCLUDES ${PROJECT_NAME}_TEST_LIBS ${PROJECT_NAME}_TEST_OPTS )
