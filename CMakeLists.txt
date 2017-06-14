
cmake_minimum_required ( VERSION 2.8.12 )

# project
project ( wilton_db CXX )
include ( ${CMAKE_CURRENT_LIST_DIR}/resources/macros.cmake )

# check deplibs cache
if ( STATICLIB_USE_DEPLIBS_CACHE )
    set ( ${PROJECT_NAME}_CACHED_STATIC_LIB_PATH ${STATICLIB_DEPLIBS_CACHE_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROJECT_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} )
    set ( ${PROJECT_NAME}_CACHED_SHARED_LIB_PATH ${STATICLIB_DEPLIBS_CACHE_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROJECT_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX} )
    if ( EXISTS ${${PROJECT_NAME}_CACHED_STATIC_LIB_PATH} )
        add_library( ${PROJECT_NAME} STATIC IMPORTED GLOBAL )
        set_target_properties( ${PROJECT_NAME} PROPERTIES IMPORTED_LOCATION ${${PROJECT_NAME}_CACHED_STATIC_LIB_PATH} )
        message ( STATUS "Using cached library: [${${PROJECT_NAME}_CACHED_STATIC_LIB_PATH}]" )
        return ( )
    elseif ( EXISTS ${${PROJECT_NAME}_CACHED_SHARED_LIB_PATH} )
        add_library( ${PROJECT_NAME} SHARED IMPORTED GLOBAL )
        set_target_properties( ${PROJECT_NAME} PROPERTIES IMPORTED_LOCATION ${${PROJECT_NAME}_CACHED_SHARED_LIB_PATH} )
        message ( STATUS "Using cached library: [${${PROJECT_NAME}_CACHED_SHARED_LIB_PATH}]" )
        return ( )
    endif ( )
endif ( )

#options
option ( ${PROJECT_NAME}_SHARED "Build shared library instead of a static one" ON )

# dep options
set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
# todo: use pkg-config
set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../external_libpq/libpq/include" CACHE STRING "Path to the PostgreSQL headers" )

# docs
option ( ${PROJECT_NAME}_ENABLE_DOCS "Generate doxyfile and exit build" OFF )
if ( ${PROJECT_NAME}_ENABLE_DOCS )
    configure_file ( ${CMAKE_CURRENT_LIST_DIR}/resources/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile )
    return ( )
endif ( )

# standalone build
if ( NOT DEFINED STATICLIB_DEPS )
    set ( STATICLIB_DEPS ${CMAKE_CURRENT_LIST_DIR}/../../staticlibs/ CACHE INTERNAL "" )    
endif ( )
if ( NOT DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY )
    set ( CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR} )
endif ( )

# dependencies
if ( NOT DEFINED WILTON_DEPS )
    set ( WILTON_DEPS ${CMAKE_CURRENT_LIST_DIR}/.. )
endif ( )
if ( NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux" ) # system dependencies implied on Linux
    wilton_db_add_subdirectory ( ${STATICLIB_DEPS}/external_sqlite )
endif ( )
wilton_db_add_subdirectory ( ${STATICLIB_DEPS}/external_libpq )
wilton_db_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
wilton_db_add_subdirectory ( ${STATICLIB_DEPS}/staticlib_orm )

set ( ${PROJECT_NAME}_DEPS
        staticlib_orm
        soci
        sqlite3
        libpq )
wilton_db_pkg_check_modules ( ${PROJECT_NAME}_DEPS_PC REQUIRED ${PROJECT_NAME}_DEPS )

# library
set ( ${PROJECT_NAME}_SRC
        ${CMAKE_CURRENT_LIST_DIR}/src/wilton_db.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/wiltoncall_db.cpp )
source_group ( "src" FILES ${${PROJECT_NAME}_SRC} )

set ( ${PROJECT_NAME}_HEADERS
        ${CMAKE_CURRENT_LIST_DIR}/include/wilton/wilton_db.h )
source_group ( "include" FILES ${${PROJECT_NAME}_HEADERS} )

if ( ${PROJECT_NAME}_SHARED )
    set ( ${PROJECT_NAME}_LIBTYPE SHARED )
else ( )
    set ( ${PROJECT_NAME}_LIBTYPE STATIC )
endif ( )

add_library ( ${PROJECT_NAME} ${${PROJECT_NAME}_LIBTYPE}
        ${${PROJECT_NAME}_SRC} 
        ${${PROJECT_NAME}_HEADERS} )
target_link_libraries ( ${PROJECT_NAME} ${${PROJECT_NAME}_DEPS_PC_LIBRARIES} )
target_include_directories ( ${PROJECT_NAME} BEFORE PRIVATE 
        ${CMAKE_CURRENT_LIST_DIR}/src
        ${CMAKE_CURRENT_LIST_DIR}/include
        ${WILTON_DEPS}/wilton_core/include
        ${${PROJECT_NAME}_DEPS_PC_INCLUDE_DIRS} )
target_compile_options ( ${PROJECT_NAME} PRIVATE ${${PROJECT_NAME}_DEPS_PC_CFLAGS_OTHER} ) 
if ( ${PROJECT_NAME}_SHARED )
    target_compile_definitions ( ${PROJECT_NAME} PRIVATE -DWILTON_SHARED -DWILTON_SHARED_IMPORT )
endif ( )

# pkg-config
set ( ${PROJECT_NAME}_PC_CFLAGS "-I${CMAKE_CURRENT_LIST_DIR}/include" )
set ( ${PROJECT_NAME}_PC_LIBS "-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY} -l${PROJECT_NAME}" )
wilton_db_list_to_string ( ${PROJECT_NAME}_PC_REQUIRES_PRIVATE "" ${PROJECT_NAME}_DEPS )
configure_file ( ${CMAKE_CURRENT_LIST_DIR}/resources/pkg-config.in 
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/pkgconfig/${PROJECT_NAME}.pc )
