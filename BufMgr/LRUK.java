/* File LRUK.java */

package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class LRUK is a subclass of class Replacer using LRUK
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {

  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */
  private int  frames[];
  final private long INFINITY = Long.MAX_VALUE;
  //History Control Block which stores the time reference of the page accessed
  private long[][] HIST;
  
  //Data Structures to store Last(P) which is the last time stamp of the Page p
  public long[] last;
  //This is the value of K used as lastReference value
  private int K;
 	
  //Corelated Reference Peroid  
  private long Correlated_Reference_Period;
  
  //Time t at which page has been referenced.
  private long currentTime;
  
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;

   
  /* back function 
  public long back(int pageNo);
  */
  
  
  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {   

	     int index;
	     for ( index=0; index < nframes; ++index )
	        if ( frames[index] == frameNo )
	            break;

	    while ( ++index < nframes )
	        frames[index-1] = frames[index];
	    
	    frames[nframes-1] = frameNo;

  	
  		
  }
  
  public void initHCB(int page){
	long atT = System.currentTimeMillis();
	if(HIST == null){
		HIST = new long[10000][K];		//allocate the page		
		//System.out.println("Page No:"+page);
		//System.out.println("HIST[page].length"+HIST[0]);
		//System.out.println("HIST[page].length"+HIST[0]);
		for(int i = 1;i<K;i++){
			HIST[page][i]=0;
		}
	}
	else{
		for(int i = 1;i<K;i++){
			HIST[page][i] = HIST[page][i-1];
		}
	}
	HIST[page][0] = atT;
	last[page] = atT;	
  }

  /**
   * Calling super class the same method
   * Initializing the frames[] with number of buffer allocated
   * by buffer manager
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
    {
    	super.setBufferManager(mgr);
		frames = new int [mgr.getNumBuffers() +1];
		nframes = 0;
		Correlated_Reference_Period = 2;	//Milli second
		last = new long [10000];
		//hcb = new long [mgr.getNumBuffers()][K];
    }

/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg, int lastRef)
    {
      super(mgrArg);
      frames = null;
      //HIST = null;
      last = null;
      currentTime = 0;
      K = lastRef;
      HIST = new long[10000][K];
    }
  
  /**
   * calll super class the same method
   * pin the page in the given frame number 
   * move the page to the end of list  
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
	if ((frameNo < 0) || (frameNo >= (int)mgr.getNumBuffers())) {
		throw new InvalidFrameNumberException (null, "BUFMGR: BAD_BUFFRAMENO.");
	}
	int pin_pgid = (mgr.frameTable())[frameNo].pageNo.pid;
	long atT = System.currentTimeMillis();
	//System.out.println("Pin_pgid : "+pin_pgid);
	if(mgr.frameTable()[frameNo].pin_count()!=0){
	if(atT - last[pin_pgid] > Correlated_Reference_Period){
		long cor_PeroidofRefdPage = last[pin_pgid] - HIST[pin_pgid][0];
		for(int i = 1;i<K;i++){
			HIST[pin_pgid][i] = HIST[pin_pgid][i-1] + cor_PeroidofRefdPage;
		}
		HIST[pin_pgid][0] = atT;
		last[pin_pgid] = atT;
	}
	else{
		last[pin_pgid] = atT;
	}   
	}
    super.pin(frameNo);    
    update(frameNo);    
 }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LRUK policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */


 public int[] getFrames(){
 	return frames;
 }
 //This function will calculate the backward distance of all the hist arrays

  public long gHIST(int page, int idx){
  	//System.out.println("Page :"+page + " Id :"+idx);
  	return HIST[page][idx];
  }
 public long back(int page, int k){
 	int i=HIST[page].length;
 	if(i < k)
 		return INFINITY;
 	
 	return (HIST[page][0] - HIST[page][k-1]);
 	
 		
 }
 public int pick_victim() throws BufferPoolExceededException
 {
   int numBuffers = mgr.getNumBuffers();
   int frame;
   int page = mgr.presentPage;
   long atT = System.currentTimeMillis();
	if(HIST == null){
		HIST = new long[10000][K];		//allocate the page		
		//System.out.println("Page No:"+page);
		//System.out.println("HIST[page].length"+HIST[0]);
		//System.out.println("HIST[page].length"+HIST[0]);
		for(int i = 0;i<K;i++){
			HIST[page][i]=0;
		}
	}
	else{
		for(int i = 1;i<K;i++){
			HIST[page][i] = HIST[page][i-1];
		}
	}
	HIST[page][0] = atT;
	last[page] = atT;	

   //System.out.print("nframes : "+nframes + " numBuffers : "+numBuffers);
    if ( nframes < numBuffers ) {
        frame = nframes++;
        //System.out.println("Frame No : "+frame);
        frames[frame] = frame;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
        //System.out.println("\t\treturning frame");
        return frame;
    }
    //System.out.println();
    //long currentTime = System.currentTimeMillis();
    
    int victim = -1;
    long min = -1,max = -1;
    int j=0,pin_pgid;
    atT = System.currentTimeMillis();
    int ent = 0, unent = 0;
    for(int i=0;i<numBuffers;++i){
    	frame = frames[i];    	
    	if(state_bit[frame].state != Pinned){
    		pin_pgid = (mgr.frameTable())[frame].pageNo.pid;
        	//System.out.println("pin_pgid : "+pin_pgid+" frame id : "+frames[i]);
        	if(!(pin_pgid < 0)){
        		if((mgr.frameTable())[frame].pin_cnt==0){
        		if (atT - last[pin_pgid] > Correlated_Reference_Period && HIST[pin_pgid][K-1] < atT){
    				//System.out.println("Algo works !! :P  max : "+max);
    				if(max <= back(pin_pgid,K)){		//Since we are taking Long.MAX_VALUE for infinity & there is no val greater than that
    					ent++;						      	//its better to add equality symbol too
    					//System.out.println("Algo works !! :P ");
    					max = back(pin_pgid,K); 
    					if (state_bit[frames[i]].state != Pinned ) {
    						victim = frames[i];
    						min = HIST[frames[i]][K-1];
    						//break;
    					}		        
    				}  	
        		}
        		}
        		else {
        			unent++;
        		}
        	}
        	else{
        		if (state_bit[frame].state != Pinned ) {
        			victim = frame;
    				//break;
        		}
        		//i++;
        	}
    	}
		//System.out.println("Entered algo :"+ent+" Not Entered : "+unent);
		//System.out.println("Victim : "+victim);
		if (victim != -1){
			if ( state_bit[victim].state != Pinned ) {
				state_bit[victim].state = Pinned;
				(mgr.frameTable())[victim].pin();
				//System.out.println("Victim"+victim);
				update(victim);
				try{
					Thread.sleep(4);
				}
				catch(Exception e){
					
				}
				return victim;
			}
			//System.out.println("Victim selected by not unpinned");
		}    	
    }
    throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");
 }
 
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LRUK"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
  
}




/*

	if(pin_pgid < 0){
    		if (state_bit[frame].state != Pinned ) {
    			//System.out.println("The frame "+frames[i]+" is not pinned.");
				victim = frame;
				//System.out.println("--"+HIST.length+"--"+HIST[frames[i]].length);
				//min = HIST[frame][K-1];
				break;
			}	
    	}

int pin_pgid = (mgr.frameTable())[frameNo].pageNo.pid;
	long atT = System.currentTimeMillis();
	if(atT - Last(pin_pgid) > Correlated_Reference_Period){
		int cor_PeroidofRefdPage = LAST(pin_pgid) - HIST(pin_pgid,1);
		for(int i = 1;i<K;i++){
			long temp = HIST(pin_pgid,i) + cor_PeroidofRefdPage;
			setHIST(pin_pgid,i,temp);
		}
		replacer.setHIST(pin_pgid,1,atT);
		replacer.setLAST(pin_pgid,atT);
	}
	else{
		replacer.setLAST(pin_pgid,atT);

/* HIST function 
  public long HIST(int page, int idx){
  	System.out.println("Page :"+page + " Id :"+idx);
  	return hcb[page][idx];
  }
  
  public void setHIST(int page,int idx,int value){
  	if(hcb == null){
  		hcb = new long[page][K]; 	
  	}
  	hcb[page][idx] = value;  		
  }
  
  public void allocateHIST(int page){
  	hcb = new long[page][K]; 
  }
  
  /* LAST Function
  public long LAST(int page){
  	return last[page];
  }
  
  public void setLAST(int page, int value){
  	this.last[page]=value;
  }
    
  /* get for Correlated Reference Page *
  public long getCorRefPeriod(){
  	return Correlated_Reference_Period;
  }

  /*setter for Page referenced time *
  public void setCurrTime(long mills){
  	this.currentTime = mills;
  }

*/
