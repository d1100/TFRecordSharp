namespace D1100.Data

module Encoding = 

    open System
    open System.Text


    let kMaskDelta = 0xa282ead8ul


    let Mask (crc: uint32) = 
      // Rotate right by 15 bits and add a constant.
      ((crc >>> 15) ||| (crc <<< 17)) + kMaskDelta


    // Return the crc whose masked representation is masked_crc.
    let Unmask (masked_crc: uint32 ) =
        let rot = masked_crc - kMaskDelta
        ((rot >>> 17) ||| (rot <<< 15))
    
    
    let EncodeFixed64 (x: uint64) =
        if BitConverter.IsLittleEndian then
            BitConverter.GetBytes(x)
        else
            BitConverter.GetBytes(x) |> Array.rev


    let EncodeFixed32 (x: uint32) =
        if BitConverter.IsLittleEndian then
            BitConverter.GetBytes(x)
        else    
            BitConverter.GetBytes(x) |> Array.rev


    let DecodeFixed64 (buf: byte[]) =
        if BitConverter.IsLittleEndian then
            BitConverter.ToUInt64(buf,0)
        else
             0UL ||| 
             (buf.[0] |> uint64) <<< 56 ||| 
             (buf.[1] |> uint64) <<< 48 ||| 
             (buf.[2] |> uint64) <<< 40 ||| 
             (buf.[3] |> uint64) <<< 32 ||| 
             (buf.[4] |> uint64) <<< 24 ||| 
             (buf.[5] |> uint64) <<< 16 ||| 
             (buf.[6] |> uint64) <<< 8  ||| 
             (buf.[7] |> uint64)


    let DecodeFixed32 (buf: byte[]) =
        if BitConverter.IsLittleEndian then
            BitConverter.ToUInt32(buf,0)
        else
            0u |||
            (buf.[0] |> uint32) <<< 24 ||| 
            (buf.[1] |> uint32) <<< 16 ||| 
            (buf.[2] |> uint32) <<< 8  ||| 
            (buf.[3] |> uint32)

module TFRecord =

    open Encoding
    open Force.Crc32

    ////////////////////////////////////////////////////////////////////////////////
    /// Pack/UnpackTFRecord
    ////////////////////////////////////////////////////////////////////////////////

    /// recLen:recLen_crc32c_masked:record:record_crc32c_mask
    /// recLen: uint32 lenth of data
    /// recLen_crc32c_masked : uint32 crc of length
    /// record: bytes[]
    /// record_crc32c_mask : uint32 crc of data
    let BytesToPacked (record: byte[]) =
        let recLen = record.Length |> uint64
        let recLenBytes = recLen |> EncodeFixed64
        let recLenCrc32cMaskedBytes = 
            Crc32CAlgorithm.Compute(recLenBytes) |> Mask |> EncodeFixed32
        let recordCrc32cMaskedBytes = 
            Crc32CAlgorithm.Compute(record) |> Mask |> EncodeFixed32
        let offset0 = 0
        let offset1 = recLenBytes.Length
        //printfn "offset1 : %d" offset1
        let offset2 = offset1 + recLenCrc32cMaskedBytes.Length
        //printfn "offset2 : %d" offset2
        let offset3 = offset2 + record.Length
        let totalLen = offset3 + recordCrc32cMaskedBytes.Length
        let packed = Array.zeroCreate<byte> totalLen
        do Array.blit recLenBytes 0 packed offset0 recLenBytes.Length
        do Array.blit recLenCrc32cMaskedBytes 0 packed offset1 recLenCrc32cMaskedBytes.Length
        do Array.blit record 0 packed offset2 record.Length
        do Array.blit recordCrc32cMaskedBytes 0 packed offset3 recordCrc32cMaskedBytes.Length
        packed

    /// header should be just the
    /// little endien uint64 and masked crc32c value of length
    /// so 16 + 8 bytes 
    let TryExtractLength (recLenbytes: byte[]) (recLenCrc32cMaskedBytes: byte[]) : option<uint64> =
        let recLen = recLenbytes |> DecodeFixed64
        let unmaskedCrcVal = recLenCrc32cMaskedBytes |> DecodeFixed32 |> Unmask
        if Crc32CAlgorithm.Compute(recLenbytes)  = unmaskedCrcVal then
            Some recLen 
        else    
            printfn "error: TryUnpackRecordLength: data corruption recLen"
            None
    
    
    let TryExtractRecord (record: byte[]) (recordCrc32cMaskedBytes: byte[]) : option<byte[]> =
        let unmaskedCrcVal = recordCrc32cMaskedBytes |> DecodeFixed32 |> Unmask
        if Crc32CAlgorithm.Compute(record) = unmaskedCrcVal then 
            Some record
        else
            printfn "error: TryExtractRecord: unmatched crc32c. corrupt"
            None




    ///////////////////////////////////////////////////////////////////////////////
    /// File IO both Zlib and non Zlib
    //////////////////////////////////////////////////////////////////////////////


    open System.IO
    open Ionic.Zlib

    /// appends
    let AppendRecord (fs: FileStream) (record: byte[]) =
        let packedRecord = record |> BytesToPacked
        do fs.Write(packedRecord,0,packedRecord.Length)
        do fs.Flush()


    let MaybeReadOneRecord (fs: FileStream) =
        let recLenBytes = Array.zeroCreate<byte> 8
        let recCRC32cLen = Array.zeroCreate<byte> 4
        if fs.Read(recLenBytes,0,8) > 0 && 
           fs.Read(recCRC32cLen,0,4) > 0 then
            match TryExtractLength recLenBytes recCRC32cLen with
            | Some(len) -> 
                let len32 = len |> int
                let record = Array.zeroCreate<byte> len32
                let recordCRC32c = Array.zeroCreate<byte> 4
                if fs.Read(record,0,len32) > 0 && 
                   fs.Read(recordCRC32c,0,4) > 0 then
                    TryExtractRecord record recordCRC32c
                else    
                    None
            | None -> None
        else
            None


    let AppendRecordZlib (zs: ZlibStream) (record: byte[]) =
        let packedRecord = record |> BytesToPacked
        do zs.Write(packedRecord,0,packedRecord.Length)
        do zs.Flush()

    let MaybeReadOneRecordZlib (zs: ZlibStream) =
        let recLenBytes = Array.zeroCreate<byte> 8
        let recCRC32cLen = Array.zeroCreate<byte> 4
        if zs.Read(recLenBytes,0,8) > 0 && 
           zs.Read(recCRC32cLen,0,4) > 0 then
            match TryExtractLength recLenBytes recCRC32cLen with
            | Some(len) -> 
                let len32 = len |> int
                let record = Array.zeroCreate<byte> len32
                let recordCRC32c = Array.zeroCreate<byte> 4
                if zs.Read(record,0,len32) > 0 && 
                   zs.Read(recordCRC32c,0,4) > 0 then
                    TryExtractRecord record recordCRC32c
                else    
                    None
            | None -> None
        else
            None

    type StreamType = FS of FileStream | ZS of ZlibStream

    let YieldRecords mapFromBytesFun fs =
        let maybeReadOneFun =
            match fs with
            | FS(stream) -> fun () -> MaybeReadOneRecord stream
            | ZS(stream) -> fun () -> MaybeReadOneRecordZlib stream
        let rec Loop acc = 
            match maybeReadOneFun () with
            | Some(ba) -> (ba |> mapFromBytesFun) :: acc |> Loop
            | None -> acc
        Loop []


    let WriteAllRecords (mapToBytesFun:'T -> byte[]) filename records =
        use fs = new FileStream(filename,FileMode.Create,FileAccess.Write)
        for record in records do
            do record |> mapToBytesFun |> AppendRecord fs

    let ReadAllRecords (mapFromBytesFun: byte[] -> 'T) filename  =
        use fs = File.OpenRead(filename)
        FS(fs) |> YieldRecords mapFromBytesFun

    let WriteAllRecordsZlibCompressed (mapToBytesFun:'T -> byte[]) filename records =
        use fs = new FileStream(filename,FileMode.Create,FileAccess.Write)
        use zlibStream = new ZlibStream(fs, CompressionMode.Compress)
        for record in records do
            do record |> mapToBytesFun |> AppendRecordZlib zlibStream

    let ReadAllRecordsZlibCompressed (mapFromBytesFun: byte[] -> 'T) filename  =
        use fs = File.OpenRead(filename)
        use zlibStream = new ZlibStream(fs, CompressionMode.Decompress)
        ZS(zlibStream) |> YieldRecords mapFromBytesFun