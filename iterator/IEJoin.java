package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;

import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;

public class IEJoin {

	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator p_i1, // pointers to the two iterators. If the
			p_i2; // inputs are sorted, then no sorting is done
	private TupleOrder _order; // The sorting order.
	private CondExpr OutputFilter[];
	private Tuple Jtuple;
	private FldSpec perm_mat[];
	private int nOutFlds;
	private int jc_in1, jc_in2;

	private short[] Rsizes;
	private AttrType[] Rtypes;

	/**
	 * constructor,initialization
	 * 
	 * @param in1[]
	 *            Array containing field types of table
	 * @param len_in1
	 *            # of columns in table
	 * @param s1_sizes
	 *            shows length of string field in table not required in our case
	 * @param join_col_in1
	 *            column of table1 to be joined with table2
	 * @param sortFld1Len
	 *            length of sorted field in table1
	 * @param join_col_in2
	 *            column of table2 to be joined with table1
	 * @param sortFld2Len
	 *            length of sorted field in table2
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left input of join
	 * @param am2
	 *            access method for right input of join
	 * @param in1_sorted
	 *            is am1 sorted?
	 * @param in2_sorted
	 *            is am2 sorted?
	 * @param order
	 *            the order of the tuple: ascending or descending?
	 * @param outFilter
	 *            Pointer to the output filter
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fields
	 * @param chunksize
	 *            chunk size for bloomfilter
	 * @exception JoinNewFailed
	 *                allocate failed
	 * @exception JoinLowMemory
	 *                memory not enough
	 * @exception SortException
	 *                exception from sorting
	 * @exception TupleUtilsException
	 *                exception from using tuple utils
	 * @exception IOException
	 *                some I/O fault
	 */
	public IEJoin(AttrType in1[], int len_in1, short s1_sizes[],

			int join_col_in1, int sortFld1Len,

			int join_col_in2, int sortFld2Len,

			int amt_of_mem, FileScan am1, FileScan am2,

			boolean in1_sorted, boolean in2_sorted,

			TupleOrder order,

			CondExpr outFilter[], FldSpec proj_list[], int n_out_flds, double chunksize)
					throws JoinNewFailed, JoinLowMemory, SortException, TupleUtilsException, IOException {

		// Assign all arguments to corresponding local variables
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in1.length]; // 2nd table

		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in1, 0, _in2, 0, in1.length); // 2nd table
		in1_len = len_in1;
		in1_len = len_in1; // 2nd table

		Jtuple = new Tuple();

		// output Attributes
		AttrType[] Jtypes = new AttrType[n_out_flds];
		perm_mat = proj_list;
		nOutFlds = n_out_flds;

		p_i1 = am1;
		p_i2 = am2; // 2nd table
		OutputFilter = outFilter;
		_order = order;
		jc_in1 = join_col_in1;
		jc_in2 = join_col_in2;

		int select_Col1 = perm_mat[0].offset;
		int select_Col2 = perm_mat[1].offset;

		Tuple tup;
		int record_Count1 = am1.getNumRecords();
		int record_Count2 = am2.getNumRecords();

		// define L1 L1' L2 L2' further used in algorithm
		Integer[][] L1 = new Integer[record_Count1][2];
		Integer[][] L2 = new Integer[record_Count1][2];
		Integer[][] L1_1 = new Integer[record_Count2][2];
		Integer[][] L2_1 = new Integer[record_Count2][2];

		int len_tab1 = 0;
		tup = new Tuple();

		// stores table for reference
		ArrayList<Tuple> initTable1 = new ArrayList<Tuple>();
		ArrayList<Tuple> initTable2 = new ArrayList<Tuple>();

		for (int i = 0; i < record_Count1; i++) {
			try {
				initTable1.add(new Tuple(am1.get_next()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		for (int i = 0; i < record_Count2; i++) {
			try {
				initTable2.add(new Tuple(am2.get_next()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// build the L1 L2 array
		for (int i = 0; i < record_Count1; i++) {
			try {
				int t1 = initTable1.get(i).getIntFld(join_col_in1);
				L1[i][0] = t1;
				L1[i][1] = i;

				int t2 = initTable1.get(i).getIntFld(join_col_in2);
				L2[i][0] = t2;
				L2[i][1] = i;
			} catch (Exception e) {
				System.err.println("Error while creating initTable");
				e.printStackTrace();
			}
		}

		// build the L1' L2' array
		for (int i = 0; i < record_Count2; i++) {
			try {
				int t1 = initTable2.get(i).getIntFld(join_col_in1);
				L1_1[i][0] = t1;
				L1_1[i][1] = i;

				int t2 = initTable2.get(i).getIntFld(join_col_in2);
				L2_1[i][0] = t2;
				L2_1[i][1] = i;
			} catch (Exception e) {
				System.err.println("Error while creating initTable");
				e.printStackTrace();
			}
		}

		// sort L1 L1' L2 L2' as per the operators line 3-6 of the algorithm
		AttrOperator[] opr = new AttrOperator[2];
		int num_of_cond = OutputFilter.length;
		for (int i = 0; i < num_of_cond; i++) {
			opr[i] = OutputFilter[0].op;
			if (opr[i].attrOperator == AttrOperator.aopGT || opr[i].attrOperator == AttrOperator.aopGE) {
				if (i == 0) {
					Arrays.sort(L1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -(e1[0].compareTo(e2[0]));
						}
					});
					Arrays.sort(L1_1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -(e1[0].compareTo(e2[0]));
						}
					});
				}
				if (i == 1) {
					Arrays.sort(L2, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return e1[0].compareTo(e2[0]);
						}
					});
					Arrays.sort(L2_1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return e1[0].compareTo(e2[0]);
						}
					});
				}
			} else if (opr[i].attrOperator == AttrOperator.aopLT || opr[i].attrOperator == AttrOperator.aopLE) {
				if (i == 0) {
					Arrays.sort(L1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return (e1[0].compareTo(e2[0]));
						}
					});
					Arrays.sort(L1_1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return (e1[0].compareTo(e2[0]));
						}
					});
				}
				if (i == 1) {
					Arrays.sort(L2, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -(e1[0].compareTo(e2[0]));
						}
					});
					Arrays.sort(L2_1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -e1[0].compareTo(e2[0]);
						}
					});
				}
			}
		}

		// Build a permutation Array line 7-8 of the algorithm
		Integer[] permut_Array1 = new Integer[record_Count1];
		for (int i = 0; i < record_Count1; i++) {
			for (int j = 0; j < record_Count1; j++) {
				int one = L1[i][1];
				int two = L2[j][1];
				if (one == two) {
					permut_Array1[j] = i;
					break;
				}
			}
		}

		Integer[] permut_Array2 = new Integer[record_Count2];
		for (int i = 0; i < record_Count2; i++) {
			for (int j = 0; j < record_Count2; j++) {
				int one = L1_1[i][1];
				int two = L2_1[j][1];
				if (one == two) {
					permut_Array2[j] = i;
					break;
				}
			}
		}

		int O1[] = new int[record_Count1];
		int O2[] = new int[record_Count1];

		// boild offset array O1 O2 line 9-10 of the algorithm
		for (int i = 0; i < num_of_cond; i++) {
			opr[i] = OutputFilter[0].op;
			if (opr[i].attrOperator == AttrOperator.aopGT || opr[i].attrOperator == AttrOperator.aopGE) {
				if (i == 0) {// Descending
					// set Offset Array O1 descending
					for (int j = 0; j < record_Count1; j++) {
						int key = L1[j][0];
						for (int k = 0; k < record_Count2; k++) {

							if (key > L1_1[k][0]) {
								if (k + 1 < record_Count2 && L1_1[k][0] != L1_1[k + 1][0]) {
									O1[j] = k;
									break;
								}
							} else
								O1[j] = record_Count2;
						}
					}

				}
				if (i == 1) {// Ascending
					// set Offset Array O2 ascending
					for (int j = 0; j < record_Count1; j++) {
						int key = L2[j][0];
						for (int k = 0; k < record_Count2; k++) {

							if (key <= L2_1[k][0]) {
								if (k + 1 < record_Count2 && L2_1[k][0] != L2_1[k + 1][0]) {
									O2[j] = k;
									break;
								}
							} else
								O2[j] = record_Count2;
						}
					}
				}
			} else if (opr[i].attrOperator == AttrOperator.aopLT || opr[i].attrOperator == AttrOperator.aopLE) {
				if (i == 0) {// Ascending
					// set Offset Array O1 ascending
					for (int j = 0; j < record_Count1; j++) {
						int key = L1[j][0];
						for (int k = 0; k < record_Count2; k++) {

							if (key <= L1_1[k][0]) {
								if (k + 1 < record_Count2 && L1_1[k][0] != L1_1[k + 1][0]) {
									O1[j] = k;
									break;
								}
							} else
								O1[j] = record_Count2;
						}
					}
				}
				if (i == 1) {// Descending
					// set Offset Array O2 descending
					for (int j = 0; j < record_Count1; j++) {
						int key = L2[j][0];
						for (int k = 0; k < record_Count2; k++) {

							if (key > L2_1[k][0]) {
								if (k + 1 < record_Count2 && L2_1[k][0] != L2_1[k + 1][0]) {
									O2[j] = k;
									break;
								}
							} else
								O2[j] = record_Count2;
						}
					}
				}
			}
		}

		// Build a Bit Array line 11 of the algorithm
		// Bloom filter chunk size;
		double chunk_size = chunksize;

		// Actual bit array
		BitSet bitArray = new BitSet(record_Count2);

		// Index bit array
		BitSet indArray = new BitSet((int) Math.ceil(record_Count2 / chunk_size));

		// Final Result Set Tuple line 12 of the algorithm
		Heapfile f = null;
		Rtypes = new AttrType[2];
		Rtypes[0] = new AttrType(AttrType.attrInteger);
		Rtypes[1] = new AttrType(AttrType.attrInteger);
		Rsizes = new short[1];
		Rsizes[0] = 0;
		Tuple t_res = new Tuple();
		try {
			t_res.setHdr((short) 2, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		int size = t_res.size();
		f = null;
		try {
			f = new Heapfile("result.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t_res = new Tuple(size);
		try {
			t_res.setHdr((short) 2, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		// set eqOff as per the operator line 13-14 of the algorithm
		int eqOff = 1;
		if ((opr[0].attrOperator == AttrOperator.aopGE || opr[0].attrOperator == AttrOperator.aopLE)
				&& (opr[1].attrOperator == AttrOperator.aopGE || opr[1].attrOperator == AttrOperator.aopLE))
			eqOff = 0;
		else
			eqOff = 1;

		int off2 = 0;
		int off1 = 0;
		Tuple t;

		// Implement the algorithm line 15-22
		for (int i = 0; i < record_Count1; i++) {
			off2 = O2[i];
			for (int j = 0; j < off2; j++) {
				bitArray.set(permut_Array2[j]);
				indArray.set((int) Math.floor(permut_Array2[j] / chunk_size));
			}
			off1 = O1[permut_Array1[i]];
			for (int k = (off1 + eqOff); (k < off1 + (chunk_size - (off1 % chunk_size))) && (k < record_Count2); k++) {

				if (bitArray.get(k) == true) {
					try {
						t = new Tuple();
						int tm1 = L2[i][1];
						int tm2 = L2_1[k][1];
						Tuple tm1_tup = initTable1.get(tm1);
						Tuple tm2_tup = initTable2.get(tm2);
						t_res.setIntFld(1, tm1_tup.getIntFld(select_Col1));
						t_res.setIntFld(2, tm2_tup.getIntFld(select_Col2));
						f.insertRecord(t_res.returnTupleByteArray());
					} catch (Exception e) {
						System.err.println("Error in Query while building result Set");
						e.printStackTrace();
					}
				}
			}
			for (int j = ((int) Math.ceil((off1 + eqOff) / chunk_size)); j < ((int) Math
					.ceil(record_Count2 / chunk_size)); j++) {
				if (indArray.get(j) == true) {
					for (int k = 0; k < chunk_size; k++) {
						int l = j * (int) chunk_size + k;
						if (l >= record_Count2)
							break;
						if (bitArray.get(l) == true) {
							try {
								t = new Tuple();
								int tm1 = L2[i][1];
								int tm2 = L2_1[k][1];
								Tuple tm1_tup = initTable1.get(tm1);
								Tuple tm2_tup = initTable2.get(tm2);
								t_res.setIntFld(1, tm1_tup.getIntFld(select_Col1));
								t_res.setIntFld(2, tm2_tup.getIntFld(select_Col2));
								f.insertRecord(t_res.returnTupleByteArray());
							} catch (Exception e) {
								System.err.println("Error in Query while building result Set");
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	public FileScan result = null;

	/**
	 * method to return the result for the join algorithm
	 * 
	 * @return access method for output of join
	 */
	public FileScan get_Result() {
		FldSpec[] Sprojection = new FldSpec[2];
		Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		int i = 0;
		result = null;
		Sort sm = null;
		try {
			result = new FileScan("result.in", Rtypes, Rsizes, (short) 2, (short) 2, Sprojection, null);
			// sm = new Sort(Rtypes, (short) 2, Rsizes, (Iterator) result, 2,
			// _order, 4, (int) 20);
			// System.out.println("Num of records : " + result.getNumRecords());
			// System.out.println("Exit*********");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

}
