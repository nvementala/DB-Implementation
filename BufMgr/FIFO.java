/* File FIFO.java */

package bufmgr;

import diskmgr.*;
import global.*;
import java.util.*;

  /**
   * class FIFO is a subclass of class Replacer using FIFO
   * algorithm for page replacement
   */
class FIFO extends  Replacer {

	/**
	* private field
	* An array to hold number of frames in the buffer pool
	*/

	private int  frames[];
	Queue queue = null;
 
	/**
	* private field
	* number of frames used
	*/   
	private int  nframes;
	private int head = 0;
	/**
	* This pushes the given frame to the end of the list.
	* @param frameNo the frame number
	*/
	private void update(int frameNo)
	{
		//queue.add(frameNo);    		
		int index;
		for ( index=0; index < nframes; ++index )
			if ( frames[index] == frameNo )
				break;

		while ( ++index < nframes )
		    frames[index-1] = frames[index];
		frames[nframes-1] = frameNo;     
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
		frames = new int [ mgr.getNumBuffers() ];
		queue = new LinkedList();
		nframes = 0;
    }

	/* public methods */

  	/**
   	* Class constructor
	* Initializing frames[] pinter = null.
	*/
    public FIFO(BufMgr mgrArg)
    {
		super(mgrArg);
		//frames = null;
		
		queue = null;
    }
  
	/**
	* call super class the same method
	* pin the page in the given frame number 
	* move the page to the end of list  
	*
	*/
	public void pin(int frameNo) throws InvalidFrameNumberException
	{
		super.pin(frameNo);
		//update(frameNo);		
	}

 public int[] getFrames(){
 	return frames;
 }
 
 
 public int pick_victim() throws BufferPoolExceededException
 {
   int numBuffers = mgr.getNumBuffers();
   int frame;
   
    if ( nframes < numBuffers ) {
        frame = nframes++;
        frames[frame] = frame;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
        //System.out.println("returning Frame");
        return frame;
    }

    /*if(queue != null){
    	int frame = queue.element;
    	state_bit[frame].state = Pinned;
    	(mgr.frameTable())[frame].pin();
    	update(frame);
    	return frame;    	
    }*/
    
    for ( int i = head; i < numBuffers; ++i ) {
         frame = frames[i];
        if ( state_bit[frame].state != Pinned ) {
            state_bit[frame].state = Pinned;
            (mgr.frameTable())[frame].pin();
            head = i;
            //update(frame);
            //System.out.println("returning Frame");
            return frame;
        }
    }
    if(head == numBuffers)	head = 0;
   //System.out.println("returning -1");
   //return -1;
   throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");
 }
 
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "FIFO"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "FIFO REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
			System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
  
}



