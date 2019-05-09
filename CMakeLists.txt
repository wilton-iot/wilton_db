# Copyright 2017, alex at staticlibs.net
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required ( VERSION 2.8.12 )

# project
project ( wilton_db CXX )

# dep options

# dependencies
set ( ${PROJECT_NAME}_ADDITIONAL_DEPS )
if ( STATICLIB_TOOLCHAIN MATCHES "(android|windows|macosx)_.+" )
    set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
    set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../external_libpq/libpq/include" CACHE STRING "Path to the PostgreSQL headers" )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_sqlite )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_libpq )
    set ( ${PROJECT_NAME}_ADDITIONAL_DEPS sqlite3 libpq )
elseif ( WILTON_BUILD_FLAVOUR MATCHES "wheezy" )
    set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
    set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../external_libpq/libpq/include" CACHE STRING "Path to the PostgreSQL headers" )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_libpq )
    set ( ${PROJECT_NAME}_ADDITIONAL_DEPS sqlite3 libpq )
elseif ( WILTON_BUILD_FLAVOUR MATCHES "raspbian_.+" )
    set ( SOCI_ENABLE_POSTGRES ON CACHE BOOL "Enable PostgreSQL backend" )
    set ( SOCI_POSTGRES_INCLUDE_DIR "${CMAKE_SYSROOT}/usr/include/postgresql" CACHE STRING "Path to the PostgreSQL headers" )
    staticlib_add_subdirectory ( ${STATICLIB_DEPS}/external_soci )
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
if ( STATICLIB_TOOLCHAIN MATCHES "windows_.+" )
    configure_file ( ${WILTON_DIR}/resources/buildres/wilton_module.rc
            ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.rc )
    set ( ${PROJECT_NAME}_RESFILE ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.rc )
    set ( ${PROJECT_NAME}_DEFFILE ${CMAKE_CURRENT_LIST_DIR}/resources/${PROJECT_NAME}.def )
endif ( )

add_library ( ${PROJECT_NAME} SHARED
        ${CMAKE_CURRENT_LIST_DIR}/src/wilton_db.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/wilton_db_psql.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/wiltoncall_db.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/psql_functions.cpp
        ${CMAKE_CURRENT_LIST_DIR}/include/wilton/wilton_db.h
        ${CMAKE_CURRENT_LIST_DIR}/include/wilton/wilton_db_psql.h
        ${${PROJECT_NAME}_RESFILE}
        ${${PROJECT_NAME}_DEFFILE} )
        
target_link_libraries ( ${PROJECT_NAME} PRIVATE
        wilton_core
        wilton_logging
        ${${PROJECT_NAME}_DEPS_PC_LIBRARIES} )

target_include_directories ( ${PROJECT_NAME} BEFORE PRIVATE 
        ${CMAKE_CURRENT_LIST_DIR}/src
        ${CMAKE_CURRENT_LIST_DIR}/include
        ${WILTON_DIR}/core/include
        ${WILTON_DIR}/modules/wilton_logging/include
        ${${PROJECT_NAME}_DEPS_PC_INCLUDE_DIRS} )
        
target_compile_options ( ${PROJECT_NAME} PRIVATE ${${PROJECT_NAME}_DEPS_PC_CFLAGS_OTHER} )

# platform-specific link options
if ( STATICLIB_TOOLCHAIN MATCHES "android_.+" )
    set_property ( TARGET ${PROJECT_NAME} APPEND_STRING PROPERTY LINK_FLAGS "-Wl,-soname,lib${PROJECT_NAME}.so" )
elseif ( STATICLIB_TOOLCHAIN MATCHES "windows_.+" )
    set_property ( TARGET ${PROJECT_NAME} APPEND_STRING PROPERTY LINK_FLAGS "/manifest:no" )
endif ( )

# mac dep paths
if ( STATICLIB_TOOLCHAIN MATCHES "macosx_.+" )
    add_custom_command ( TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${WILTON_DIR}/resources/scripts/mac-deps.sh
                    ${CMAKE_SHARED_LIBRARY_PREFIX}${PROJECT_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
            WORKING_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
            COMMENT "Rewriting dependency paths: [${CMAKE_SHARED_LIBRARY_PREFIX}${PROJECT_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}]" )
endif ( )

# debuginfo
staticlib_extract_debuginfo_shared ( ${PROJECT_NAME} )

# pkg-config
set ( ${PROJECT_NAME}_PC_CFLAGS "-I${CMAKE_CURRENT_LIST_DIR}/include" )
set ( ${PROJECT_NAME}_PC_LIBS "-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY} -l${PROJECT_NAME}" )
staticlib_list_to_string ( ${PROJECT_NAME}_PC_REQUIRES_PRIVATE "" ${PROJECT_NAME}_DEPS )
configure_file ( ${WILTON_DIR}/resources/buildres/pkg-config.in 
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/pkgconfig/${PROJECT_NAME}.pc )
