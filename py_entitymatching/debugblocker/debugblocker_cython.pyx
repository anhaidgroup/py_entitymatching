from libcpp.vector cimport vector
from libcpp.set cimport set as cset
from libcpp.map cimport map as cmap
from libcpp.pair cimport pair as cpair
from libcpp.queue cimport priority_queue as heap
from libcpp.string cimport string
from libcpp cimport bool
from libc.stdio cimport printf, fprintf, fopen, fclose, FILE, sprintf
from cython.parallel import prange, parallel
import time
from joblib import Parallel, delayed

ctypedef unsigned int uint

cdef extern from "GenerateRecomLists.h" nogil:
    cdef cppclass RecPair:
        RecPair()
        RecPair(int, int, int)
        int l_rec, r_rec, rank;

    cdef cppclass GenerateRecomLists:
        GenerateRecomLists()
        
        vector[RecPair] generate_recom_lists(vector[vector[int]]& ltoken_vector, vector[vector[int]]& rtoken_vector,
                              vector[vector[int]]& lindex_vector, vector[vector[int]]& rindex_vector,
                              vector[int]& ltoken_sum_vector, vector[int]& rtoken_sum_vector, vector[int]& field_list,
                              cmap[int, cset[int]]& cand_set, double field_remove_ratio,
                              uint output_size);

        vector[vector[int]]  generate_config(vector[int]& field_list, vector[int]& ltoken_sum_vector, 
                              vector[int]& rtoken_sum_vector, double
                              field_remove_ratio, uint lvector_size, uint
                              rvector_size);
        
        vector[vector[int]] sort_config(vector[vector[int]]& config_lists);

        cmap[cpair[int, int], int] generate_topk_with_config(vector[int]& config, vector[vector[int]]& ltoken_vector, vector[vector[int]]& rtoken_vector,
                              vector[vector[int]]& lindex_vector, vector[vector[int]]& rindex_vector,
                              cmap[int, cset[int]]& cand_set, uint output_size);

        vector[RecPair] merge_topk_lists(vector[cmap[cpair[int, int], int]]& rec_lists);


FIELD_REMOVE_RATIO = 0.1


def debugblocker_cython(lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list,
                        ltable_field_token_sum, rtable_field_token_sum, py_cand_set,
                        py_num_fields, py_output_size):

    ### Convert py objs to c++ objs
    cdef vector[vector[int]] ltoken_vector, rtoken_vector
    convert_table_to_vector(lrecord_token_list, ltoken_vector)
    convert_table_to_vector(rrecord_token_list, rtoken_vector)

    cdef vector[vector[int]] lindex_vector, rindex_vector
    convert_table_to_vector(lrecord_index_list, lindex_vector)
    convert_table_to_vector(rrecord_index_list, rindex_vector)

    cdef vector[int] ltoken_sum, rtoken_sum
    convert_py_list_to_vector(ltable_field_token_sum, ltoken_sum)
    convert_py_list_to_vector(rtable_field_token_sum, rtoken_sum)

    cdef cmap[int, cset[int]] cand_set
    convert_candidate_set_to_c_map(py_cand_set, cand_set)

    cdef vector[int] field_list
    for i in range(py_num_fields):
        field_list.push_back(i)

    cdef uint output_size = py_output_size
    cdef double field_remove_ratio = FIELD_REMOVE_RATIO

    del lrecord_token_list, rrecord_token_list
    del lrecord_index_list, rrecord_index_list
    del py_cand_set


    cdef int max_field_num = field_list.size()

    cdef GenerateRecomLists generator

    cdef vector[RecPair] rec_list;
    rec_list = generator.generate_recom_lists(ltoken_vector, rtoken_vector, lindex_vector, rindex_vector,
                          ltoken_sum, rtoken_sum, field_list,
                          cand_set, field_remove_ratio,
                          output_size);
    py_rec_list = []
    for i in xrange(rec_list.size()):
        py_rec_list.append((rec_list[i].rank, rec_list[i].l_rec, rec_list[i].r_rec))
    
    return py_rec_list


def debugblocker_config_cython(ltable_field_token_sum, rtable_field_token_sum,
                        py_num_fields, py_ltable_size, py_rtable_size):

    ### Convert py objs to c++ objs
    cdef vector[int] ltoken_sum, rtoken_sum
    convert_py_list_to_vector(ltable_field_token_sum, ltoken_sum)
    convert_py_list_to_vector(rtable_field_token_sum, rtoken_sum)

    cdef vector[int] field_list
    for i in range(py_num_fields):
        field_list.push_back(i)

    cdef double field_remove_ratio = FIELD_REMOVE_RATIO

    cdef int max_field_num = field_list.size()
    cdef uint ltable_size = py_ltable_size
    cdef uint rtable_size = py_rtable_size

    cdef GenerateRecomLists generator

    cdef vector[vector[int]] config_lists;
    config_lists = generator.generate_config(field_list, ltoken_sum, rtoken_sum, 
                                            field_remove_ratio,
                                            ltable_size, rtable_size);
    config_lists = generator.sort_config(config_lists);
    py_config_list = []
    for i in range(config_lists.size()):
        tmp_list = []
        for j in range(config_lists[i].size()):
            tmp_list.append(config_lists[i][j])
        py_config_list.append(tmp_list)

    return py_config_list


def debugblocker_topk_cython(py_config, lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list,
                        py_cand_set, py_output_size):

    ### Convert py objs to c++ objs
    cdef vector[int] config
    convert_py_list_to_vector(py_config, config)

    cdef vector[vector[int]] ltoken_vector, rtoken_vector
    convert_table_to_vector(lrecord_token_list, ltoken_vector)
    convert_table_to_vector(rrecord_token_list, rtoken_vector)

    cdef vector[vector[int]] lindex_vector, rindex_vector
    convert_table_to_vector(lrecord_index_list, lindex_vector)
    convert_table_to_vector(rrecord_index_list, rindex_vector)

    cdef cmap[int, cset[int]] cand_set
    convert_candidate_set_to_c_map(py_cand_set, cand_set)

    cdef uint output_size = py_output_size

    del lrecord_token_list, rrecord_token_list
    del lrecord_index_list, rrecord_index_list
    del py_cand_set

    cdef GenerateRecomLists generator

    cdef cmap[cpair[int, int], int] rec_list;
    rec_list = generator.generate_topk_with_config(config, ltoken_vector, rtoken_vector,
                              lindex_vector, rindex_vector, cand_set, output_size);
   
    py_rec_list = []
    for it in rec_list:
        py_rec_list.append([it.first.first, it.first.second, it.second])
    return py_rec_list


def debugblocker_merge_topk_cython(py_rec_lists):
    cdef vector[cmap[cpair[int, int], int]] rec_lists
    convert_py_list_to_vector_map(py_rec_lists, rec_lists)

    cdef GenerateRecomLists generator

    cdef vector[RecPair] rec_list;
    rec_list = generator.merge_topk_lists(rec_lists);

    py_rec_list = []
    for i in xrange(rec_list.size()):
        py_rec_list.append((rec_list[i].rank, rec_list[i].l_rec, rec_list[i].r_rec))

    return py_rec_list
    
        
cdef void convert_py_list_to_vector_map(py_rec_lists, 
        vector[cmap[cpair[int, int], int]]& rec_lists):
    cdef cmap[cpair[int, int], int] rec_list
    for py_rec_list in py_rec_lists:
        for l_rec, r_rec, rank in py_rec_list:
            rec_list[[l_rec, r_rec]] = rank
        rec_lists.push_back(rec_list)
        rec_list.clear()


cdef void convert_table_to_vector(table_list, vector[vector[int]]& table_vector):
    cdef int i, j
    for i in range(len(table_list)):
        table_vector.push_back(vector[int]())
        for j in range(len(table_list[i])):
            table_vector[i].push_back(table_list[i][j])


cdef void convert_candidate_set_to_c_map(cand_set, cmap[int, cset[int]]& new_set):
    cdef int key, value
    for key in cand_set:
        if not new_set.count(key):
            new_set[key] = cset[int]()

        l = cand_set[key]
        for value in l:
            new_set[key].insert(value)


cdef int convert_py_list_to_vector(py_list, vector[int]& vector):
    for value in py_list:
        vector.push_back(value)
