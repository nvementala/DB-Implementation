package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IESelfJoin extends Iterator implements GlobalConst {
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
	private int _amt_of_mem;
	private short[] Rsizes;
	private AttrType[] Rtypes;

	// IESelfJoin - Double Predicate
	public IESelfJoin(AttrType in1[], int len_in1, short s1_sizes[],

			int join_col_in1, int sortFld1Len,

			int join_col_in2, int sortFld2Len,

			int amt_of_mem, FileScan am1,

			boolean in1_sorted,

			TupleOrder order,

			CondExpr outFilter[], FldSpec proj_list[], int n_out_flds, double chunkSize)
					throws JoinNewFailed, JoinLowMemory, SortException, TupleUtilsException, IOException {

		// Assign all your arguments to local variables
		_in1 = new AttrType[in1.length];
		in1_len = len_in1;
		_amt_of_mem = amt_of_mem;
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		in1_len = len_in1;
		_order = order;

		Jtuple = new Tuple();

		// output Attributes
		AttrType[] Jtypes = new AttrType[n_out_flds];
		// short[] ts_size = null;
		perm_mat = proj_list;
		nOutFlds = n_out_flds;

		// try{
		// ts_size =
		// TupleUtils.setup_op_tuple(Jtuple,Jtypes,in1,len_in1,s1_sizes,proj_list,n_out_flds);
		// }
		// catch(Exception e){
		// System.out.print("Exception : "+e);
		// }

		p_i1 = am1;
		OutputFilter = outFilter;
		_order = order;
		jc_in1 = join_col_in1;
		jc_in2 = join_col_in2;

		int select_Col1 = perm_mat[0].offset;
		int select_Col2 = perm_mat[1].offset;
		// This has sub-table of original table
		// Replace the Tuple with Integer
		HashMap<Integer, Integer> org_table1 = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> org_table2 = new HashMap<Integer, Integer>();
		Tuple tup;
		int record_Count = am1.getNumRecords();
		Integer[][] L1 = new Integer[record_Count][2];
		Integer[][] L2 = new Integer[record_Count][2];
		// ArrayList<Integer> L1 = new ArrayList<Integer>();
		int len_tab1 = 0;
		tup = new Tuple();
		// System.out.println("Tuples are :");
		ArrayList<Tuple> initTable = new ArrayList<Tuple>();
		// int i=0;
		/*
		 * while(tup!=null){ try{ tup = new Tuple(am1.get_next());
		 * if(tup!=null){
		 * 
		 * initTable.add(tup); }
		 * 
		 * }catch(Exception e){ e.printStackTrace(); } }
		 */

		for (int i = 0; i < record_Count; i++) {
			try {
				initTable.add(new Tuple(am1.get_next()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for (int i = 0; i < record_Count; i++) {
			try {
				int t1 = initTable.get(i).getIntFld(join_col_in1);
				L1[i][0] = t1;
				L1[i][1] = i;

				int t2 = initTable.get(i).getIntFld(join_col_in2);
				L2[i][0] = t2;
				L2[i][1] = i;
				// System.out.println(" --- " + t1 + " : " + t2);
				// +tup.getIntFld(4));
			} catch (Exception e) {
				System.err.println("Error while creating initTable");
				e.printStackTrace();
			}
		}

		AttrOperator[] opr = new AttrOperator[2];
		int num_of_cond = OutputFilter.length;
		for (int i = 0; i < num_of_cond; i++) {
			opr[i] = OutputFilter[0].op;
			if (opr[i].attrOperator == AttrOperator.aopGT || opr[i].attrOperator == AttrOperator.aopGE) {
				if (i == 0) {// Make changes to L1 first time
					Arrays.sort(L1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return e1[0].compareTo(e2[0]);
						}
					});
				}
				if (i == 1) {// Make changes to L2 second time
					Arrays.sort(L2, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return e1[0].compareTo(e2[0]);
						}
					});
				}
			} else if (opr[i].attrOperator == AttrOperator.aopLT || opr[i].attrOperator == AttrOperator.aopLE) {
				if (i == 0) {// Make changes to L1 first time
					Arrays.sort(L1, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -(e1[0].compareTo(e2[0]));
						}
					});
				}
				if (i == 1) {// Make changes to L2 second time
					Arrays.sort(L2, new Comparator<Integer[]>() {
						@Override
						public int compare(final Integer[] e1, final Integer[] e2) {
							return -(e1[0].compareTo(e2[0]));
						}
					});
				} // Arrays.sort(L2,Collections.reverseOrder()); // Make changes
					// to L2 second time
			}
		}
		// Build a permutation Array
		Integer[] permut_Array = new Integer[record_Count];
		for (int i = 0; i < record_Count; i++) {
			for (int j = 0; j < record_Count; j++) {
				int t1 = L1[i][1];
				int t2 = L2[j][1];
				if (t1 == t2) {
					permut_Array[j] = i;
					break;
				}
			}
		}

		// Bloom filter chunk size
		double chunk_size = chunkSize;

		// Actual Bit Array
		BitSet bitArray = new BitSet(record_Count);

		// Index bit array
		BitSet indArray = new BitSet((int) Math.ceil(record_Count / chunk_size));

		// Final Result Set Tuple
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

		int eqOff = 1;
		if ((opr[0].attrOperator == AttrOperator.aopGE || opr[0].attrOperator == AttrOperator.aopLE)
				&& (opr[1].attrOperator == AttrOperator.aopGE || opr[1].attrOperator == AttrOperator.aopLE))
			eqOff = 0;
		else
			eqOff = 1;

		int predicate1_eqOff = 1;
		if ((opr[0].attrOperator == AttrOperator.aopGE || opr[0].attrOperator == AttrOperator.aopLE))
			predicate1_eqOff = 0;

		int predicate2_eqOff = 1;
		if (opr[1].attrOperator == AttrOperator.aopGE || opr[1].attrOperator == AttrOperator.aopLE)
			predicate2_eqOff = 0;
		// ArrayList<Tuple> resultSet

		// Implement the algorithm (11-16) => Normal algo without considering
		// Bloom Filter
		// Tuple t;
		// for(int i=0;i<record_Count;i++){
		// int pos = permut_Array[i];
		// bitArray.set(pos);
		// for(int j=(pos+eqOff);j<record_Count;j++){
		// if(bitArray.get(j) == true){
		// try{
		// t = new Tuple();
		// int tm1 = L1[j][1];
		// int tm2 = L1[permut_Array[i]][1];
		// Tuple tm1_tup = initTable.get(tm1);
		// Tuple tm2_tup = initTable.get(tm2);
		//
		// //duplicate check
		// if(tm1_tup.getIntFld(join_col_in1)==tm2_tup.getIntFld(join_col_in1)
		// && predicate1_eqOff!=1)
		// continue;
		//
		// if(tm1_tup.getIntFld(join_col_in2)==tm2_tup.getIntFld(join_col_in2)
		// && predicate2_eqOff!=1)
		// continue;
		//
		//// System.out.println(tm1_tup.getIntFld(1)+","+tm1_tup.getIntFld(2)+","+tm1_tup.getIntFld(3)+","
		//// +tm1_tup.getIntFld(4));
		//// System.out.println(tm2_tup.getIntFld(1)+","+tm2_tup.getIntFld(2)+","+tm2_tup.getIntFld(3)+","
		//// +tm2_tup.getIntFld(4));
		//
		// t_res.setIntFld(1, tm1_tup.getIntFld(select_Col1));
		// t_res.setIntFld(2, tm2_tup.getIntFld(select_Col2));
		// f.insertRecord(t_res.returnTupleByteArray());
		// }
		// catch(Exception e){
		// System.err.println("Error in Query_2b while building result Set");
		// e.printStackTrace();
		// }
		// }
		// }
		// }

		// Optimized version
		Tuple t;
		for (int i = 0; i < record_Count; i++) {
			int pos = permut_Array[i];
			bitArray.set(pos);
			indArray.set((int) Math.floor(pos / chunk_size));
			for (int j = (pos + eqOff); j < (pos + (chunk_size - (pos % chunk_size))) && (j < record_Count); j++) {
				if (bitArray.get(j) == true) {
					try {
						t = new Tuple();
						int tm1 = L1[j][1];
						int tm2 = L1[permut_Array[i]][1];
						Tuple tm1_tup = initTable.get(tm1);
						Tuple tm2_tup = initTable.get(tm2);

						// duplicate check
						if (tm1_tup.getIntFld(join_col_in1) == tm2_tup.getIntFld(join_col_in1) && predicate1_eqOff != 1)
							continue;

						if (tm1_tup.getIntFld(join_col_in2) == tm2_tup.getIntFld(join_col_in2) && predicate2_eqOff != 1)
							continue;

						// System.out.println(tm1_tup.getIntFld(1) + "," +
						// tm1_tup.getIntFld(2) + ","
						// + tm1_tup.getIntFld(3) + "," + tm1_tup.getIntFld(4));
						// System.out.println(tm2_tup.getIntFld(1) + "," +
						// tm2_tup.getIntFld(2) + ","
						// + tm2_tup.getIntFld(3) + "," + tm2_tup.getIntFld(4));

						t_res.setIntFld(1, tm1_tup.getIntFld(select_Col1));
						t_res.setIntFld(2, tm2_tup.getIntFld(select_Col2));
						f.insertRecord(t_res.returnTupleByteArray());
					} catch (Exception e) {
						System.err.println("Error in Query_2b while building result Set");
						e.printStackTrace();
					}
				}
			}
			for (int j = ((int) Math.ceil((pos + eqOff) / chunk_size)); j < ((int) Math
					.ceil(record_Count / chunk_size)); j++) {
				if (indArray.get(j) == true) {
					for (int k = 0; k < chunk_size; k++) {
						int l = j * (int) chunk_size + k;
						if (l >= record_Count)
							break;
						if (bitArray.get(l) == true) {
							try {
								t = new Tuple();
								int tm1 = L1[l][1];
								int tm2 = L1[permut_Array[i]][1];
								Tuple tm1_tup = initTable.get(tm1);
								Tuple tm2_tup = initTable.get(tm2);

								// duplicate check
								if (tm1_tup.getIntFld(join_col_in1) == tm2_tup.getIntFld(join_col_in1)
										&& predicate1_eqOff != 1)
									continue;

								if (tm1_tup.getIntFld(join_col_in2) == tm2_tup.getIntFld(join_col_in2)
										&& predicate2_eqOff != 1)
									continue;

								// System.out.println(tm1_tup.getIntFld(1) + ","
								// + tm1_tup.getIntFld(2) + ","
								// + tm1_tup.getIntFld(3) + "," +
								// tm1_tup.getIntFld(4));
								// System.out.println(tm2_tup.getIntFld(1) + ","
								// + tm2_tup.getIntFld(2) + ","
								// + tm2_tup.getIntFld(3) + "," +
								// tm2_tup.getIntFld(4));

								t_res.setIntFld(1, tm1_tup.getIntFld(select_Col1));
								t_res.setIntFld(2, tm2_tup.getIntFld(select_Col2));
								f.insertRecord(t_res.returnTupleByteArray());
							} catch (Exception e) {
								System.err.println("Error in Query_2b while building result Set");
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	// Single Predicate Working - Don't Touch this
	public IESelfJoin(AttrType in1[], int len_in1, short s1_sizes[],

			int join_col_in1, int sortFld1Len,

			int amt_of_mem, FileScan am1,

			boolean in1_sorted,

			TupleOrder order,

			CondExpr outFilter[], FldSpec proj_list[], int n_out_flds)
					throws JoinNewFailed, JoinLowMemory, SortException, TupleUtilsException, IOException {

		// Assign all your arguments to local variables
		_in1 = new AttrType[in1.length];

		System.arraycopy(in1, 0, _in1, 0, in1.length);
		in1_len = len_in1;

		Jtuple = new Tuple();

		// output Attributes
		AttrType[] Jtypes = new AttrType[n_out_flds];
		// short[] ts_size = null;
		perm_mat = proj_list;
		nOutFlds = n_out_flds;

		// try{
		// ts_size =
		// TupleUtils.setup_op_tuple(Jtuple,Jtypes,in1,len_in1,s1_sizes,proj_list,n_out_flds);
		// }
		// catch(Exception e){
		// System.out.print("Exception : "+e);
		// }

		p_i1 = am1;
		OutputFilter = outFilter;
		_order = order;
		jc_in1 = join_col_in1;
		// jc_in2 = join_col_in2;

		int col1 = perm_mat[0].offset;
		int col2 = perm_mat[1].offset;
		// This has sub-table of original table
		// Replace the Tuple with Integer
		HashMap<Integer, Integer> org_table1 = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> org_table2 = new HashMap<Integer, Integer>();
		Tuple tup;
		int record_Count = am1.getNumRecords();
		System.out.println("Record Count is : " + record_Count);
		Integer[] L1 = new Integer[record_Count];
		// ArrayList<Integer> L1 = new ArrayList<Integer>();
		int len_tab1 = 0;
		tup = new Tuple();
		for (int i = 0; i < record_Count; i++) {
			try {
				tup = am1.get_next();
				int t1 = tup.getIntFld(join_col_in1);
				int val = tup.getIntFld(col1);
				L1[i] = t1;
				org_table1.put(t1, val);
				// System.out.println(
				// tup.getIntFld(1) + "," + tup.getIntFld(2) + "," +
				// tup.getIntFld(3) + "," + tup.getIntFld(4));
				// tup = null;
			} catch (Exception e) {
				System.out.println("Exception at filling the table");
				e.printStackTrace();
			}
		}

		if (col1 != col2) {
			for (int i = 0; i < record_Count; i++) {
				try {
					tup = am1.get_next();
					int t1 = tup.getIntFld(join_col_in1);
					int val = tup.getIntFld(col2);
					L1[i] = t1;
					org_table2.put(t1, val);
					// System.out.println(tup.getIntFld(1) + "," +
					// tup.getIntFld(2) + "," + tup.getIntFld(3) + ","
					// + tup.getIntFld(4));
					// tup = null;
				} catch (Exception e) {
					System.out.println("Exception at filling the table");
					e.printStackTrace();
				}
			}
		} else {
			// System.out.println("Columns were same, I have copied the same
			// thing");
			org_table2 = org_table1;
		}

		/*
		 * while(tup != null){ try{ tup = am1.get_next(); if(tup != null){ int
		 * t1 = tup.getIntFld(join_col_in1); //System.out.println(t1);
		 * org_table.put(t1, tup);
		 * System.out.println(tup.getIntFld(1)+","+tup.getIntFld(2)+","+tup.
		 * getIntFld(3)+"," +tup.getIntFld(4));
		 * 
		 * L1[len_tab1]=t1; len_tab1++; //L1.add(e) tup=new Tuple(); }
		 * }catch(Exception e){ System.out.print("Exception : "+e+"\n"); } }
		 * 
		 * for(int x=0; x<3;x++){
		 * 
		 * int tu=org_table.get(L1[x]); try { //System.out.println(L1[x]+" "
		 * +tu.getIntFld(1)+","+tu.getIntFld(2)+","+tu.getIntFld(3)+","); }
		 * catch (FieldNumberOutOfBoundException e) { // TODO Auto-generated
		 * catch block e.printStackTrace(); } tup=null; }
		 */

		AttrOperator opr = OutputFilter[0].op;

		if (opr.attrOperator == AttrOperator.aopGT || opr.attrOperator == AttrOperator.aopGE) {
			Arrays.sort(L1);
		} else if (opr.attrOperator == AttrOperator.aopLT || opr.attrOperator == AttrOperator.aopLE) {
			Arrays.sort(L1, Collections.reverseOrder());
		}
		int eqOff = 0;

		if (opr.attrOperator == AttrOperator.aopGE || opr.attrOperator == AttrOperator.aopLE)
			eqOff = 1;

		Heapfile f = null;
		Rtypes = new AttrType[2];
		Rtypes[0] = new AttrType(AttrType.attrInteger);
		Rtypes[1] = new AttrType(AttrType.attrInteger);
		Rsizes = new short[1];
		Rsizes[0] = 0;
		Tuple t = new Tuple();
		try {
			t.setHdr((short) 2, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "boats"
		// RID rid;
		f = null;
		try {
			f = new Heapfile("result.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 2, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		// ArrayList<Tuple> resultSet
		for (int i = 0; i < record_Count; i++) {
			for (int j = 0; j < (i + eqOff); j++) {
				try {
					if (eqOff != 1 && L1[i] == L1[j])
						continue;
					int tm1 = org_table1.get(L1[i]);
					int tm2 = org_table2.get(L1[j]);
					t.setIntFld(1, tm1);
					t.setIntFld(2, tm2);
					// System.out.println(tm1 + " " + tm2);
					RID rid = f.insertRecord(t.returnTupleByteArray());
				} catch (Exception e) {
					System.err.println("*** error in Heapfile.insertRecord() ***");
					e.printStackTrace();
				}
			}
		}
	}

	public FileScan result = null;

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
			System.out.println("Num of records : " + result.getNumRecords());
			System.out.println("Exit*********");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub

	}

}