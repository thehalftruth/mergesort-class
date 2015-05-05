/*
Class Name: merge_sort_worker
Description: A class for sorting BIG files
Author: Adrian Haider
Create date: 21.01.2015
Last changed: 05.05.2015
Version: 0.1.1.0
License: GNU GPLv3 (https://www.gnu.org/licenses/gpl.txt)

This program comes with ABSOLUTELY NO WARRANTY

WARNING: code contains C++11 */

#ifndef MERGESORT_CPP
#define MERGESORT_CPP

#include <string>
#include <fstream>
#include <vector>
#include <stdexcept>
#include <exception>
#include <sstream>
#include <ios>
#include <stdint.h>

#include <iostream>
#include <assert.h>

#include "mergesort.h"
#include "safegetline.h"
#include <boost/filesystem.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace boost_filesys = boost::filesystem;

merge_sort_worker::merge_sort_worker(const boost_filesys::path _infile,
                                     const boost_filesys::path _outfile,
                                     const boost_filesys::path _chunk_path,
                                     const sortmode _sort_mode,
                                     const bool _delete_double_lines,
                                     const unsigned int _lines_per_file,
                                     const std::string _newline
                                     ) {
    set_infile(_infile);
    set_outfile(_outfile);
    set_chunk_path(_chunk_path);
    set_lines_per_file(_lines_per_file);
    set_sort_mode(_sort_mode);
    set_lineend(_newline);

    lines_before = 0;
    lines_after = 0;
}

merge_sort_worker::~merge_sort_worker() {};

bool merge_sort_worker::ready(void) const {
    return infile_path.empty() == false &&
           outfile_path.empty() == false &&
           chunk_path.empty() == false;
}

// set-methods
void merge_sort_worker::set_infile(const boost_filesys::path path) {
    infile_path = path;
}

void merge_sort_worker::set_infile(const std::string path) {
    infile_path = boost_filesys::path(path);
}

void merge_sort_worker::set_infile(const char* path) {
    infile_path = boost_filesys::path(path);
}


void merge_sort_worker::set_outfile(const boost_filesys::path path) {
    outfile_path = path;
}

void merge_sort_worker::set_outfile(const std::string path) {
    outfile_path = boost_filesys::path(path);
}

void merge_sort_worker::set_outfile(const char* path) {
    outfile_path = boost_filesys::path(path);
}


void merge_sort_worker::set_chunk_path(const boost_filesys::path path) {
    chunk_path = path;
}

void merge_sort_worker::set_chunk_path(const std::string path) {
    chunk_path = boost_filesys::path(path);
}

void merge_sort_worker::set_chunk_path(const char* path) {
    chunk_path = boost_filesys::path(path);
}


void merge_sort_worker::set_sort_mode(const sortmode sm) {
    sort_mode = sm;
}

bool merge_sort_worker::set_lines_per_file(unsigned int n)
{
    if (n == 0) {
        return true;
    }
    
    lines_per_chunk = n;
    return false;
}

bool merge_sort_worker::set_lineend(std::string _lineend)
{
    if (_lineend == "")
    {
        newline = "\n";
        return true;
    }

    newline = _lineend;

    return false;
}

bool merge_sort_worker::set_lineend(const char* _lineend)
{
    if (strcmp(_lineend, ""))
    {
        newline = "\n";
        return true;
    }

    newline = _lineend;

    return false;
}

void merge_sort_worker::set_delete_double_lines(bool ddl) {
    delete_double_lines = ddl;
}

// get-methods
uint32_t merge_sort_worker::get_lines_before(void) const {
    return lines_before;
}

uint32_t merge_sort_worker::get_lines_after(void) const {
    return lines_after;
}

uint32_t merge_sort_worker::get_deleted_lines(void) const {
    return lines_before - lines_after;
}

inline void merge_sort_worker::tidy_up(std::vector<std::string>& string_vector) const
{
    /* remove double strings from vector */

    if (delete_double_lines == false) {
        return;
    }
    
    std::vector<std::string> help_vector;
    bool found;
    
    for (std::string & line : string_vector)
    {
        if (line.empty()) { // delete empty lines
            continue;
        }
        
        found = false;

        for (std::string & j : help_vector) // check if line is more than one time in vector
        {
            if (line == j) {
                found = true;
                break;
            }
        }

        if (found == false) {
            help_vector.push_back(line);
        }
    }

    if (string_vector.size() != help_vector.size()) { // don't copy if nothing changes
        string_vector = help_vector;
    }
}

std::string merge_sort_worker::file_get_line(boost_filesys::path path,
                                        uint64_t pos, uint64_t & next_pos) const
{
    std::ifstream infile;
    std::string line;

    infile.open(path.c_str(), std::ios::binary);

    if (infile.is_open() == false) {
        throw std::runtime_error("Could not open file");
    }

    infile.seekg(pos);         // set read position
    safeGetline(infile, line);
    next_pos = infile.tellg(); // get read position

    return line;
}

inline short merge_sort_worker::compare_lines(std::string & string1, std::string & string2) const
{
    switch (sort_mode) {
    case (ASC) :
        return (string1.compare(string2) > 0);
        // no break required because return exits function
    case(DESC) :
        return (string1.compare(string2) < 0);
        // no break required because return exits function
    }

    return -1; // strings are equal
}

void merge_sort_worker::init_variables(void) {
    chunk_filenames.clear();

    lines_before = 0;
    lines_after = 0;
}

// todo: rename funktion
// todo: error handling
void merge_sort_worker::file_to_chunks(void)
{
    init_variables();

    std::vector<std::string> chunk;
    unsigned int chunk_filename_counter = 0;

    std::ifstream infile;
    std::ofstream chunk_file;
    std::string line, filename;
    boost_filesys::path chunk_filename;

    bool end_of_file;
    path_seek path_seek_tmp;

    // TODO: create directory

    infile.open(infile_path.c_str());

    if (infile.is_open() == false) {
        throw std::runtime_error("Open infile failed");
    }

    while (true)
    {
        end_of_file = safeGetline(infile, line).eof();

        try 
        {
            if (end_of_file == false) {
                chunk.push_back(line);
            }

            ++lines_before; // count lines for statistics

            // create chunk file if chunk size is reached or 
            // end of infile is reached
            if (chunk.size() == lines_per_chunk || end_of_file)
            {
                tidy_up(chunk);

                std::sort(chunk.begin(), chunk.end());

                // TODO: do in sort algorithm
                if (sort_mode == DESC) {
                    std::reverse(chunk.begin(), chunk.end());
                }

                // if in chunk are no double values
                if (chunk.size() == lines_per_chunk || end_of_file)
                {
                    // TODO: generate file name in function

                    std::ostringstream convert;
                    convert << chunk_filename_counter;
                    filename = convert.str();
                    filename += ".cnk"; // TODO: Macro

                    chunk_filename = chunk_path;
                    chunk_filename.append(filename);

                    // chunk read position 0
                    path_seek_tmp = { chunk_filename, 0 };
                    chunk_filenames.push_back(path_seek_tmp);

                    chunk_file.open(chunk_filename.string(), std::ios_base::binary); // TODO throw exception if open fails

                    if (chunk_file.is_open() == false) {
                        throw std::runtime_error("Could not open file");
                    }

                    for (const std::string &line : chunk)
                    {
                        // TODO: write lines in string before write to file

                        chunk_file << line << std::endl; // replace with newline

                        if (chunk_file.good() == false) {
                            throw std::runtime_error("File write error");
                        }
                    }

                    chunk_file.close();
                    chunk.clear();
                    ++chunk_filename_counter;
                }
            }
        }
        catch (std::bad_alloc &exc)
        {
            // close last file if is open

            if (chunk_file.is_open()) {
                chunk_file.close();
            }

            infile.close();

            // delete all chunk files
            for (path_seek & path_seek_tmp : chunk_filenames) {
                boost_filesys::remove(path_seek_tmp.path);
            }

            throw exc;
        }

        if (end_of_file) {
            break;
        }
    }

    infile.close();
}

void merge_sort_worker::start_merge_sort(void)
{
    std::ofstream outfile;
    std::string last_line, next_line, line;
    uint64_t tmp_next_line_pos;
    uint64_t next_line_pos;
    path_seek next_line_path_seek;

    last_line = "";

    /* binary to prevent converting \n to \r\n (or \r\n to \r\r\n) */
    outfile.open(outfile_path.string(), std::ios_base::binary);

    try {
        while (! chunk_filenames.empty())
        {
            //get next line
            std::list<path_seek>::iterator filename_iter = chunk_filenames.begin();
            std::list<path_seek>::iterator filename_end_iter = chunk_filenames.end();
            std::list<path_seek>::iterator next_line_iter;
        
            // first line is next_line on start of the loop
            next_line = file_get_line((*filename_iter).path,
                (*filename_iter).pos, tmp_next_line_pos);

            next_line_path_seek = (*filename_iter);
            next_line_iter = filename_iter;
            next_line_pos = tmp_next_line_pos;

            if (filename_iter != filename_end_iter) {
                ++filename_iter;
            } else {
                // TODO: error description
                throw std::logic_error("");
                break;
            }

            int compare_result = 0;

            for (; filename_iter != filename_end_iter; ++filename_iter)
            {
                line = file_get_line(filename_iter->path,
                    filename_iter->pos, tmp_next_line_pos);

                // compare lines
                
                compare_result = compare_lines(next_line, line);
                
                if ( compare_result == 1
                     || (delete_double_lines == false && compare_result == -1) )
                {    // next line found or don't delete double lines
                    next_line = line;
                    next_line_path_seek = (*filename_iter);
                    next_line_iter = filename_iter;
                    next_line_pos = tmp_next_line_pos;
                }
                else if (compare_result == -1)
                {   // lines are equal and delete double lines
                    filename_iter->pos = tmp_next_line_pos;   // skip line by set read position
                }
            }

            if (delete_double_lines == false || next_line != last_line)
            {
                // TODO: write to buffer
                outfile << next_line << newline;
                last_line = next_line;
                ++lines_after; // count lines for statistics
            }

            next_line_iter->pos = next_line_pos;   // set file read position
            next_line_path_seek.pos = next_line_pos;

            // delete empty chunk files

            if (boost_filesys::file_size(next_line_path_seek.path) <= next_line_path_seek.pos)
            {
                boost_filesys::remove(next_line_path_seek.path); // delete file

                // delete iterator

                filename_iter = chunk_filenames.begin();
                filename_end_iter = chunk_filenames.end();

                for (; filename_iter != filename_end_iter; ++filename_iter) {
                    if (filename_iter->path == next_line_path_seek.path) {
                        chunk_filenames.erase(filename_iter);
                        break;
                    }
                }
            }
        }
    } catch (std::bad_alloc &exc) {
        outfile.close();
        throw exc;
    } catch (std::runtime_error &exc) {
        outfile.close();
        throw exc;
    }

    outfile.close();

    // delete all remaining chunk files, in case of an error
    if (chunk_filenames.empty() == false)
    {
        for (const path_seek & path_seek_tmp : chunk_filenames) {
            boost_filesys::remove(path_seek_tmp.path);
        }
        
        throw std::logic_error("Chunk files left after sorting");
    }
}

void merge_sort_worker::start_worker(void)
{
    if (ready()) {
        file_to_chunks();
        start_merge_sort();
    } else {
        throw std::runtime_error("Path's not valid");
    }
}

void merge_sort_worker::operator() () {
    start_worker();
}

#endif MERGESORT_CPP
