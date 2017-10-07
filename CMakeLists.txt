
cmake_minimum_required ( VERSION 2.8.12 )

# project
project ( wilton_db CXX )

# dep options

# dependencies
set ( ${PROJECT_NAME}_ADDITIONAL_DEPS )
if ( STATICLIB_TOOLCHAIN MATCHES "windows_[^_]+_[^_]+" )
    set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
    set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../external_libpq/libpq/include" CACHE STRING "Path to the PostgreSQL headers" )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_sqlite )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_libpq )
    set ( ${PROJECT_NAME}_ADDITIONAL_DEPS sqlite3 libpq )
endif ( )
if ( WILTON_BUILD_FLAVOUR MATCHES "wheezy" )
    set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
    set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../external_libpq/libpq/include" CACHE STRING "Path to the PostgreSQL headers" )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_libpq )
    set ( ${PROJECT_NAME}_ADDITIONAL_DEPS sqlite3 libpq )
endif ( )

staticlib_add_subdirectory ( ${STATICLIB_DEPS}/staticlib_orm )

set ( ${PROJECT_NAME}_DEPS
        staticlib_utils
        staticlib_orm
        soci 
        ${${PROJECT_NAME}_ADDITIONAL_DEPS} )
staticlib_pkg_check_modules ( ${PROJECT_NAME}_DEPS_PC REQUIRED ${PROJECT_NAME}_DEPS )

# library
set ( ${PROJECT_NAME}_RESFILE )
set ( ${PROJECT_NAME}_DEFFILE )
if ( ${CMAKE_SYSTEM_NAME} STREQUAL "Windows" )
    configure_file ( ${WILTON_DIR}/resources/buildres/wilton_module.rc
            ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.rc )
    set ( ${PROJECT_NAME}_RESFILE ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.rc )
    set ( ${PROJECT_NAME}_DEFFILE ${CMAKE_CURRENT_LIST_DIR}/resources/${PROJECT_NAME}.def )
endif ( )

add_library ( ${PROJECT_NAME} SHARED
        ${CMAKE_CURRENT_LIST_DIR}/src/wilton_db.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/wiltoncall_db.cpp
        ${CMAKE_CURRENT_LIST_DIR}/include/wilton/wilton_db.h
        ${${PROJECT_NAME}_RESFILE}
        ${${PROJECT_NAME}_DEFFILE} )
        
target_link_libraries ( ${PROJECT_NAME} PRIVATE
        wilton_core
        ${${PROJECT_NAME}_DEPS_PC_LIBRARIES} )

target_include_directories ( ${PROJECT_NAME} BEFORE PRIVATE 
        ${CMAKE_CURRENT_LIST_DIR}/src
        ${CMAKE_CURRENT_LIST_DIR}/include
        ${WILTON_DIR}/core/include
        ${${PROJECT_NAME}_DEPS_PC_INCLUDE_DIRS} )
        
target_compile_options ( ${PROJECT_NAME} PRIVATE ${${PROJECT_NAME}_DEPS_PC_CFLAGS_OTHER} )

if ( ${CMAKE_SYSTEM_NAME} STREQUAL "Windows" )
    set_property ( TARGET ${PROJECT_NAME} APPEND_STRING PROPERTY LINK_FLAGS "/manifest:no" )
endif ( )

# pkg-config
set ( ${PROJECT_NAME}_PC_CFLAGS "-I${CMAKE_CURRENT_LIST_DIR}/include" )
set ( ${PROJECT_NAME}_PC_LIBS "-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY} -l${PROJECT_NAME}" )
staticlib_list_to_string ( ${PROJECT_NAME}_PC_REQUIRES_PRIVATE "" ${PROJECT_NAME}_DEPS )
configure_file ( ${WILTON_DIR}/resources/buildres/pkg-config.in 
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/pkgconfig/${PROJECT_NAME}.pc )
