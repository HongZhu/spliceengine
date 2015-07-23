package com.splicemachine.stats.cardinality;

import com.google.common.primitives.Ints;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.hash.Hash64;
import com.splicemachine.stats.DoubleFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * A Sparsely-stored, bias-adjusted HyperLogLog Counter.
 * <p/>
 * <p>This implementation takes advantage of small cardinalities to both
 * be more memory efficient and potentially to increase the overall accuracy of
 * the estimation.</p>
 * <p/>
 * <p>When the observed cardinality is very small, then most registers are empty,
 * resulting in a considerable waste of space, particularly for higher accuracy. To
 * avoid this situation, this implementation begins by storing data in a sparse integer
 * array, where the first few bits of the integer stores the index of the register,
 * and the remaining bits store the value. Of course, if the number of non-empty
 * registers grows beyond a certain point, the sparse representation becomes less
 * memory-efficient than a dense one, so the implementation will automatically switch
 * to a dense implementation after that point.</p>
 * <p/>
 * <p>Our sparse encoding is as follows. Let {@code p} be the precision desired. Then,
 * we know that {@code idx} is the register to which the value belongs. Set {@code p(w)}
 * to be the leftmost 1-bit in the hashed value.</p>
 * <p/>
 * <p>We know from algorithm analysis that we can store {@code p(w)} in 6 bits, but we require
 * {@code p} bits to store {@code idx}. Thus, we need at least {@code p+6} bits to store the
 * sparse representation.</p>
 * <p/>
 * <p>Since {@code p+6} is not likely to be a full word of any size, we will end up wasting at least
 * some bits. Why not waste them storing real data? Instead of storing p bits for the index, store
 * {@code idx' = wordSize-6} bits of the hash. This results in more registers, and thus better accuracy,
 * without using any storage space that we weren't using already.</p>
 * <p/>
 * <p>Furthermore, we now have a situation where {@code p(w)} may already stored in the {@code idx'} value
 * (if there is a 1-bit contains somewhere in the least significant {wordSize-p}). In that case, we don't
 * need to store the value at all, we know it already. Thus, if we use the least-significant bit to indicate
 * whether or not {@code p(w)} is present, we can save ourselves the necessity of storing the same value every time.
 * This takes one bit, so we store one less than wordSize. Thus, we end up with a format of
 * <p/>
 * idx' | p(w) | 1
 * <p/>
 * if we needed to store {@code p(w)}. Otherwise, we end up with
 * <p/>
 * idx' | 0
 * <p/>
 * Thus, for an int, we have 25-bits for {@code idx'}, 1 bit for the indicator field, and 6 bits for {@code p(w)}.</p>
 * <p/>
 * <p>When we go to add an entry, we first compose an int with the value. If the least-significant bit is 0, then
 * all we need to do is ensure that the register is present, since all values to that bucket will have the same
 * p(w). Otherwise, we will need to do a comparison on the 6-bits {@code p(w)} value to keep the maximum.</p>
 * <p/>
 * <p>Finally, we keep this array sorted, which allows us to efficiently convert between the sparse and
 * dense representations when necessary. When we go to convert to a dense representation, however, we will
 * down-convert the precision to be whatever the user calls for (so if the user asks for precision 10, we will
 * use a dense representation with precision 10, not 25), so some precision is lost at that point. However, no
 * precision is lost if the sparse representation is kept all the way through.</p>
 * <p/>
 * <p>In truth, one notices that, if the sparse representation is maintained throughout all updates and an estimate
 * is asked for, then the cardinality is low enough that fewer than {@code 3*2^(precision-6)} registers were filled,
 * which is generally well below the empirical threshold at which it's more accurate to use Linear counting anyway.
 * Thus, we can immediately perform linear counting without needing to perform the hyperloglog estimate at all.</p>
 * <p/>
 * <p>Updates to a sparse structure are relatively expensive, however. To this end, this implementation buffers
 * results up to a threshold before flushing those changes to the sparse structure in a single pass. By default, this
 * buffer threshold is set to be 25% of the maximum sparse register size (25% of {@code 3*2^(precision-6)}</p
 *
 * @author Scott Fines
 *         Date: 1/2/14
 */
public class SparseAdjustedHyperLogLogCounter extends BaseBiasAdjustedHyperLogLogCounter{
    private static final int PRECISION_BITS=25;
    private static final int REGISTER_SHIFT=Integer.SIZE-PRECISION_BITS;
    private boolean isSparse=true;

    /**
     * data held in these registers when the mode is dense. This occurs when
     * 3*2^(p-6) < sparse.length (e.g. when the memory consumed by the sparse representation
     * is more than what would be consumed by the dense).
     */
    private byte[] denseRegisters;

    private int[] sparse;

    /*
     * Because inserting into the sparse list is expensive (we have to keep it sorted), when you go
     * to update a register, you really just add it to this buffer. When the buffer fills up, the sparse
     * array is updated in bulk, saving repeated operations against the same buckets. This buffer is generally
     * set to a small value, but allowed to grow to 25% of the maximum sparse size (although the maximum is adjustable).
     */
    private int[] buffer;
    private int bufferPos=0;

    private final int maxSparseSize;
    private int maxBufferSize;
    private int sparseSize=0;

    public static SparseAdjustedHyperLogLogCounter adjustedCounter(int precision,Hash64 hashFunction){
        return new SparseAdjustedHyperLogLogCounter(precision,hashFunction,
                HyperLogLogBiasEstimators.biasEstimate(precision));
    }

    public SparseAdjustedHyperLogLogCounter(int precision,Hash64 hashFunction,
                                            DoubleFunction biasAdjuster){
        this(precision,2,-1,hashFunction,biasAdjuster);
    }

    public SparseAdjustedHyperLogLogCounter(int precision,int initialSize,int maxBufferSize,
                                            Hash64 hashFunction){
        super(precision,hashFunction);

        maxSparseSize=3*(1<<(precision-6));
        initialize(initialSize,maxBufferSize);
    }

    public SparseAdjustedHyperLogLogCounter(int precision,int initialSize,int maxBufferSize,
                                            Hash64 hashFunction,
                                            DoubleFunction biasAdjuster){
        super(precision,hashFunction,biasAdjuster);
        maxSparseSize=3*(1<<(precision-6));

        initialize(initialSize,maxBufferSize);
    }

    /*Serialization constructors*/
    private SparseAdjustedHyperLogLogCounter(int precision,
                                             int sparseSize,
                                             int[] sparse,
                                             Hash64 hashFunction,
                                             DoubleFunction biasAdjuster){
        super(precision,hashFunction,biasAdjuster);
        this.sparseSize=sparseSize;
        maxSparseSize=3*(1<<precision-6);
        this.sparse=sparse;
        this.maxBufferSize=maxSparseSize/4;
        this.buffer=new int[maxBufferSize];
    }

    private SparseAdjustedHyperLogLogCounter(int precision,
                                             byte[] denseRegisters,
                                             Hash64 hashFunction,
                                             DoubleFunction biasAdjuster){
        super(precision,hashFunction,biasAdjuster);
        this.isSparse=false;
        this.denseRegisters=denseRegisters;
        maxSparseSize=3*(1<<precision-6);
    }

    /* ****************************************************************************************************************/
    /*Modifiers*/
    @Override
    protected void doUpdate(long hash){
        if(!isSparse){
            super.doUpdate(hash);
            return;
        }

        int k = encodeHash(hash);
        buffer[bufferPos]=k;
        bufferPos++;
        if(bufferPos>=buffer.length){
            resizeOrMergeBuffer();
        }
    }


    @Override
    protected void updateRegister(int register,int value){
        byte curr=denseRegisters[register];
        if(curr>=value) return;
        denseRegisters[register]=(byte)(value&0xff);
    }

    /* ****************************************************************************************************************/
    /*Accessors*/
    @Override
    public long getEstimate(){
        if(isSparse){
            if(bufferPos>0)
                merge();
            if(isSparse){
                //count zero registers and use linear counting for our estimate
                int mP=(1<<PRECISION_BITS);
                int numZeroRegisters=mP-sparseSize; //the number of missing entries is the number of zero registers
                return (long)(mP*Math.log((double)mP/numZeroRegisters));
            }else
                return super.getEstimate();
        }else
            return super.getEstimate();
    }

    @Override
    public BaseLogLogCounter getClone(){
        if(isSparse)
            return new SparseAdjustedHyperLogLogCounter(precision,sparseSize,sparse,hashFunction,biasAdjuster);
        else
            return new SparseAdjustedHyperLogLogCounter(precision,denseRegisters,hashFunction,biasAdjuster);
    }

    @Override
    protected int getRegister(int register){
        return denseRegisters[register];
    }

    /* ****************************************************************************************************************/
		/*private helper functions*/

    private int encodeHash(long hash){
        //get the first 25 bits of hash
        int r = (int)(hash>>>(Long.SIZE-precision));
        r<<=7;

        long w = hash<<precision;
        int p = w==0l?1: (Long.numberOfLeadingZeros(w)+1);
        r|=p;
        return r;
    }

    private void initialize(int initialSize,int maxBufferSize){
        if(maxBufferSize<0)
            maxBufferSize=Math.max(initialSize,maxSparseSize/4); //default to 25% of the max sparseSize
        if(initialSize>=maxSparseSize){
            isSparse=false;
            this.denseRegisters=new byte[numRegisters];
        }else{
            this.sparse=new int[initialSize];
            this.maxBufferSize=maxBufferSize;
            if(initialSize<maxBufferSize)
                this.buffer=new int[initialSize];
            else
                this.buffer=new int[maxBufferSize];
        }
    }

    private void resizeOrMergeBuffer(){
        if(buffer.length==maxBufferSize){
            merge();
        }else{
            int[] newBuffer=new int[Math.min(buffer.length*3/2,maxBufferSize)];
            System.arraycopy(buffer,0,newBuffer,0,buffer.length);
            buffer=newBuffer;
        }
    }

    private void merge(){
		/*
		 * Merge the buffer into the sparse array.
		 *
		 * This is done in 4 stages:
		 *
		 * 1. sort buffer
		 * 2. insert into sparse array in sorted order
		 * 3. if sparse array size is too large, convert to dense and empty sparse array
		 * 4. empty buffer.
		 */
        Arrays.sort(buffer,0,bufferPos);

        int sparsePos=0;
        int bufferPointer=0;
        while(isSparse && bufferPointer<bufferPos){
            int bufferEntry=buffer[bufferPointer];
            int bufferRegister = bufferEntry>>>7;
//            int bufferRegister=bufferEntry>>>REGISTER_SHIFT;
//            boolean containsP=(bufferEntry&1)!=0;

            if(sparsePos>=sparseSize){
                insertIntoSparse(sparsePos,bufferEntry);
            }else{
                int sparseEntry=sparse[sparsePos];
                int sparseRegister = sparseEntry>>>7;
//                int sparseRegister=sparseEntry>>>REGISTER_SHIFT;
                int compare=Ints.compare(bufferRegister,sparseRegister);
                while(sparseEntry!=0 && sparsePos<sparseSize && compare>0){
                    sparsePos++;
                    if(sparsePos==sparseSize)
                        break;
//
                    sparseEntry=sparse[sparsePos];
                    sparseRegister = sparseEntry>>>7;
//                    sparseRegister=sparseEntry>>>REGISTER_SHIFT;
                    compare=Ints.compare(bufferRegister,sparseRegister);
                }

                if(compare<0 || sparsePos>=sparseSize || sparseEntry==0){
                    insertIntoSparse(sparsePos,bufferEntry);
                }else if(compare==0){
                    int bP=(bufferEntry&0x7E);
                    int sP=(sparseEntry&0x7E);
                    if(bP>sP){
                        sparse[sparsePos]=bufferEntry;
                    }
                }
//                }else if(!containsP){
//                    //replace sparse with the buffer entry if the p(w) is higher
//                    int bP=(bufferEntry&0x7E);
//                    int sP=(sparseEntry&0x7E);
//                    if(bP>sP){
//                        sparse[sparsePos]=bufferEntry;
//                    }
//                }
            }
            bufferPointer++;
        }
        for(int i=bufferPointer;i<bufferPos;i++){
            int bufferEntry=buffer[i];
            if(isSparse){
                insertIntoSparse(sparsePos,bufferEntry);
                sparsePos++;
            }else
                insertIntoDense(bufferEntry);
        }
        if(!isSparse){
            sparse=null;
            buffer=null;
        }

        this.bufferPos=0;
    }

    private void insertIntoDense(int bufferEntry){
        //first 7 bits is the p value
        byte p = (byte)(bufferEntry & 0x7F);
        int register = bufferEntry>>>7;
        int current=denseRegisters[register];
        if(current<p)
            denseRegisters[register]=p;
    }

    private void insertIntoSparse(int pos,int entry){
        if(sparseSize>=maxSparseSize){
            convertToDense();
            insertIntoDense(entry);
            return;
        }
        sparseSize++;

        if(sparseSize>sparse.length){
            //resize sparse array
            int[] newSparse=new int[Math.min(sparse.length*2,maxSparseSize)];
            System.arraycopy(sparse,0,newSparse,0,Math.min(sparse.length,pos+1));
            sparse=newSparse;
        }
        if(sparseSize>pos+1)
            System.arraycopy(sparse,pos,sparse,pos+1,sparseSize-1-pos);
        sparse[pos]=entry;
    }

    private void convertToDense(){
		/*
		 * Convert the sparse representation into a dense one. This is done
		 * by iterating through, finding the proper registers and p values, and inserting them
		 */
        isSparse=false;
        denseRegisters=new byte[numRegisters];
        for(int i=0;i<sparseSize;i++){
            insertIntoDense(sparse[i]);
        }
    }

    /*
     * Inner class to maintain the logic for serialization/deserialization
     */
    static class EncoderDecoder implements Encoder<SparseAdjustedHyperLogLogCounter>{
        private final Hash64 hashFunction;

        public EncoderDecoder(Hash64 hashFunction){
            this.hashFunction=hashFunction;
        }

        @Override
        public void encode(SparseAdjustedHyperLogLogCounter item,DataOutput encoder) throws IOException{
            if(item.isSparse){
                item.merge();
                if(item.isSparse){
                    encodeSparse(item,encoder);
                }
            }
            encodeDense(item,encoder);
        }

        @Override
        public SparseAdjustedHyperLogLogCounter decode(DataInput input) throws IOException{
            if(input.readByte()==0x01){
                return decodeSparse(input);
            }else return decodeDense(input);
        }

        private SparseAdjustedHyperLogLogCounter decodeSparse(DataInput input) throws IOException{
            int precision=input.readInt();
            int size=input.readInt();
            int c=1;
            while(c<size){
                c<<=1;
            }
            int[] sparse=new int[c];
            for(int i=0;i<size;i++){
                sparse[i]=input.readInt();
            }
            DoubleFunction biasAdjuster=HyperLogLogBiasEstimators.biasEstimate(precision);
            return new SparseAdjustedHyperLogLogCounter(precision,size,sparse,hashFunction,biasAdjuster);
        }

        private SparseAdjustedHyperLogLogCounter decodeDense(DataInput input) throws IOException{
            int precision=input.readInt();
            int numRegisters=1<<precision;
            byte[] registers=new byte[numRegisters];
            input.readFully(registers);

            DoubleFunction biasAdjuster=HyperLogLogBiasEstimators.biasEstimate(precision);
            return new SparseAdjustedHyperLogLogCounter(precision,registers,hashFunction,biasAdjuster);
        }

        private void encodeSparse(SparseAdjustedHyperLogLogCounter item,DataOutput encoder) throws IOException{
            encoder.writeByte(0x01);
            encoder.writeInt(item.precision);
            encoder.writeInt(item.sparseSize);
            for(int i=0;i<item.sparseSize;i++){
                encoder.writeInt(item.sparse[i]);
            }
        }

        private void encodeDense(SparseAdjustedHyperLogLogCounter item,DataOutput encoder) throws IOException{
            encoder.writeByte(0x00);
            encoder.writeInt(item.precision);
            //we don't need the register length, because we can reconstruct it from the precision
            encoder.write(item.denseRegisters);
        }
    }

}
