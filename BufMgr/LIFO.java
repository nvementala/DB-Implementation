/* File LIFO.java */

package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class LIFO is a subclass of class Replacer using LIFO
   * algorithm for page replacement
   */
public class LIFO extends  Replacer {

  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

    private int  frames[];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {
  	int index;
    //int numBuffers=mgr.getNumBuffers();
    for ( index=0; index < nframes; ++index )
        if ( frames[index] < 0  ||  frames[index] == frameNo )
            break;

    if ( frames[index] < 0 )
        frames[index] = frameNo;

    int frame = frames[index];
    while ( index-- >0)
      frames[index+1] = frames[index];
        
	frames[0] = frame;
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
	nframes = 0;
     }

/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LIFO(BufMgr mgrArg)
    {
      super(mgrArg);
      frames = null;
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
    super.pin(frameNo);
    //update(frameNo);    
 }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LIFO policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */


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
        return frame;
    }
    
	for(int i=0;i < numBuffers; i++){
        frame = frames[i];
        if ( state_bit[frame].state != Pinned ) {
            state_bit[frame].state = Pinned;
            (mgr.frameTable())[frame].pin();
            update(frame);
            return frame;
        }
    }

    //return -1;
    throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");
 }
 
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LIFO"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LIFO REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
  
}



