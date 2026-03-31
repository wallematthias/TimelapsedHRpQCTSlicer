cmake_minimum_required(VERSION 3.16.3...3.19.7 FATAL_ERROR)

project(TimelapsedHRpQCTSlicer)

set(EXTENSION_HOMEPAGE "https://github.com/wallematthias/TimelapsedHRpQCT")
set(EXTENSION_CATEGORY "Bone")
set(EXTENSION_CONTRIBUTORS "Matthias Walle (University of Calgary)")
set(EXTENSION_DESCRIPTION "3D Slicer GUI wrapper for timelapsed-hrpqct.")
set(EXTENSION_ICONURL "")
set(EXTENSION_SCREENSHOTURLS "")
set(EXTENSION_DEPENDS "")

find_package(Slicer REQUIRED)
include(${Slicer_USE_FILE})

add_subdirectory(TimelapsedHRpQCT)

include(${Slicer_EXTENSION_GENERATE_CONFIG})
include(${Slicer_EXTENSION_CPACK})
