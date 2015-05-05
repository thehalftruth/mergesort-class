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

#ifndef MERGESORT_H
#define MERGESORT_H

#define ASC  static_cast<sortmode>(false)
#define DESC static_cast<sortmode>(true)

#define DEFAUlT_CHUNK_PATH ""
#define DEFAULT_LINES_PER_FILE 32768
#define DEFAULT_LINE_END "\n"
#define DEFAULT_SORT_MODE ASC
#define DEFAULT_DELETE_DOUBLE_LINES false

#include <string>
#include <vector>
#include <stdint.h>

#include <boost/filesystem.hpp>

namespace boost_filesys = boost::filesystem;

typedef bool sortmode;

struct path_seek {
    boost_filesys::path path;
    uint64_t pos; // file read position
};

class merge_sort_worker {
public:
    // Constructors:
    merge_sort_worker(const boost_filesys::path _infile = "",
                      const boost_filesys::path _outfile = "",
                      const boost_filesys::path _chunk_path = DEFAUlT_CHUNK_PATH,
                      const sortmode _sort_mode = DEFAULT_SORT_MODE,
                      const bool _delete_double_lines = DEFAULT_DELETE_DOUBLE_LINES,
                      const unsigned int _lines_per_file = DEFAULT_LINES_PER_FILE,
                      const std::string _newline = DEFAULT_LINE_END
                      );

    virtual ~merge_sort_worker();

    bool ready(void) const;

    void start_worker(void);
    void operator() ();

    // set-methods
    void set_infile(boost_filesys::path);
    void set_infile(std::string);
    void set_infile(const char*);

    void set_outfile(boost_filesys::path);
    void set_outfile(std::string);
    void set_outfile(const char*);

    void set_chunk_path(boost_filesys::path);
    void set_chunk_path(std::string);
    void set_chunk_path(const char*);

    void set_sort_mode(const sortmode);

    bool set_lines_per_file(unsigned int);

    bool set_lineend(std::string);
    bool set_lineend(const char*);

    void set_delete_double_lines(bool);

    // get-methods
    uint32_t get_lines_before(void) const;
    uint32_t get_lines_after(void) const;
    uint32_t get_deleted_lines(void) const;
private:
    inline void tidy_up(std::vector<std::string>&) const;
    inline std::string file_get_line(const boost_filesys::path path,
                                uint64_t pos, uint64_t & next_pos) const;
    inline short compare_lines(std::string &, std::string &) const;

    void init_variables(void);
    void file_to_chunks(void);
    void start_merge_sort(void);

    // parameter
    boost_filesys::path infile_path;
    boost_filesys::path outfile_path;
    boost_filesys::path chunk_path;
    unsigned int lines_per_chunk;
    bool delete_double_lines;
    std::string newline;
    sortmode sort_mode;

    // for programm
    std::list<path_seek> chunk_filenames;
    uint32_t lines_before;
    uint32_t lines_after;
};

#endif MERGESORT_H