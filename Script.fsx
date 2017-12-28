///////////////////////////////////////////////////////////////////////////////
/// tests
//////////////////////////////////////////////////////////////////////////////
#I "bin/Release"
#r "DotNetZip.dll"
#r "Google.Protobuf.dll"
#r "WikiPricesPB.dll"
#r "TFRecordSharp.dll"

open System.Text

open D1100.Data

// test1
let testFilename = "test_raw_string.dat"
let rec1 = [ "merry xmas" ]
let mapToBytesFun = fun (s: string) -> Encoding.UTF8.GetBytes(s)
do rec1 |> TFRecord.WriteAllRecords mapToBytesFun testFilename

let mapFromBytesFun = fun (ba: byte[]) -> Encoding.UTF8.GetString(ba)
let rec1Restored = TFRecord.ReadAllRecords mapFromBytesFun testFilename
rec1 = rec1Restored

// test2
let testFilename2 = "test_raw_string2.dat"
let recs2 = ["merry xmas"; "&"; "a happy new year"]
do recs2 |> TFRecord.WriteAllRecords mapToBytesFun testFilename2

let recs2Restored = TFRecord.ReadAllRecords mapFromBytesFun testFilename2 |> List.rev
recs2 = recs2Restored

// test with zlib compression
let recsZlib = List.append ([ 1 .. 1024*1024 ] |> List.map (sprintf "%d very very very")) [ "merry"; "christmas"]
do recsZlib |> TFRecord.WriteAllRecords mapToBytesFun "test_large_string_no_zlib.dat"
do recsZlib |> TFRecord.WriteAllRecordsZlibCompressed mapToBytesFun "test_large_string_zlib.dat"

let recsZlibNoCompresionRestored = TFRecord.ReadAllRecords mapFromBytesFun "test_large_string_no_zlib.dat" |> List.rev
recsZlib = recsZlibNoCompresionRestored

let recsZlibRestored = TFRecord.ReadAllRecordsZlibCompressed mapFromBytesFun "test_large_string_zlib.dat" |> List.rev
recsZlib = recsZlibRestored

///////////////////////////////////////////////////////////////////////////////
// protocol buffer tests
///////////////////////////////////////////////////////////////////////////////

open System.IO
open Ionic.Zip
open Google.Protobuf

let scriptdir = __SOURCE_DIRECTORY__
let wikipricefile =  scriptdir + "/data/WIKI_PRICES_212b326a081eacca455e13140d7bb9db.zip"

let ParseDoubleOrNaN (wholeString: string) (posi: int) (doubleStr:string) =
    let res = System.Double.NaN
    match doubleStr |> System.Double.TryParse with
    | (true,x ) ->
        x
    | (false,_) ->
        printfn "warn: error parsing double in position %d:" posi
        printfn "\t%s" wholeString
        System.Double.NaN
        

let WikiPriceEODToPBList (wikipricepath:string) =
    let zf = ZipFile.Read(wikipricepath)
    let priceEntry = zf.Entries |> Seq.exactlyOne
    use zreader = priceEntry.OpenReader()
    use sr = new StreamReader(zreader)
    do sr.ReadLine() |> ignore // sluff the header
    let epoch0 = new System.DateTime(1970,1,1) // only creat obj once for date conv
    let rec loop acc =
        if sr.EndOfStream then
            acc
        else
            let li = sr.ReadLine()
            let lia = li.Split([|','|])
            let wp = new WikiDailyOHLCV()
            wp.Ticker <- lia.[0] // 0 ticker A
            wp.Ts <- System.DateTime.Parse(lia.[1]).Subtract(epoch0).TotalSeconds |> int64 // 1 date 1999-11-18
            wp.Open <- lia.[2] |> ParseDoubleOrNaN li 2 // 2 open 45.5
            wp.High <- lia.[3] |> ParseDoubleOrNaN li 3 // 3 high 50.0
            wp.Low <- lia.[4] |> ParseDoubleOrNaN li 4 // 4 low 40.0
            wp.Close <- lia.[5] |> ParseDoubleOrNaN li 5 // 5 close 44.0
            wp.Volume <- lia.[6] |> ParseDoubleOrNaN li 6 // 6 volume 44739900.0
            wp.ExDividend <- lia.[7] |> ParseDoubleOrNaN li 7 // 7 ex-dividend 0.0
            wp.SplitRatio <- lia.[8] |> ParseDoubleOrNaN li 8 // 8 split_ratio 1.0
            wp.AdjOpen <- lia.[9] |> ParseDoubleOrNaN li 9 // 9 adj_open 31.041951216877
            wp.AdjHigh <- lia.[10] |> ParseDoubleOrNaN li 10 // 10 adj_high 34.112034304261
            wp.AdjLow <- lia.[11] |> ParseDoubleOrNaN li 11 // 11 adj_low 27.289627443409
            wp.AdjClose <- lia.[12] |> ParseDoubleOrNaN li 12 // 12 adj_close 30.018590187749
            wp.AdjVolume <- lia.[13] |> ParseDoubleOrNaN li 13 // 13 adj_volume 44739900.0
            wp :: acc |> loop
    [] |> loop



let mapPBToBytesFun = fun pb -> MessageExtensions.ToByteString(pb).ToByteArray()
let mapBytesToPBFun = fun (ba: byte[]) -> WikiDailyOHLCV.Parser.ParseFrom(ba)

let wps = WikiPriceEODToPBList wikipricefile

let wp = wps.[0]
do [wp] |> TFRecord.WriteAllRecords mapPBToBytesFun "test_wp_single_pb_no_zlib.dat"
let wpNoZlibRestored = TFRecord.ReadAllRecords mapBytesToPBFun "test_wp_single_pb_no_zlib.dat" |> Seq.exactlyOne
wp = wpNoZlibRestored

do [wp] |> TFRecord.WriteAllRecordsZlibCompressed mapPBToBytesFun "test_wp_single_pb_zlib.dat"
let wpZlibRestored = TFRecord.ReadAllRecordsZlibCompressed mapBytesToPBFun "test_wp_single_pb_zlib.dat" |> Seq.exactlyOne
wp = wpZlibRestored

///

do wps |> TFRecord.WriteAllRecords mapPBToBytesFun "test_wp_all_pb_no_zlib.dat"

do wps |> TFRecord.WriteAllRecordsZlibCompressed mapPBToBytesFun "test_wp_all_pb_zlib.dat"






/////////////////////////////////////////////
// more simple unit tests
let foo = "merry xmas"
let fooUTF8Bytes = Encoding.UTF8.GetBytes(foo)
let packed = fooUTF8Bytes |> TFRecord.BytesToPacked
let len = match TFRecord.TryExtractLength packed.[0..7] packed.[8 .. 11] with | Some(i) -> i |> int32
let record = match TFRecord.TryExtractRecord packed.[12 .. (12 + len - 1)] packed.[(12 + len) ..] with | Some(ba) -> ba
foo = Encoding.UTF8.GetString(record)


    
