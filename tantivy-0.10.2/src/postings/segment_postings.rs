use crate::common::BitSet;
use crate::common::HasLen;
use crate::common::{BinarySerializable, VInt};
use crate::docset::{DocSet, SkipResult};
use crate::positions::PositionReader;
use crate::postings::compression::{compressed_block_size, AlignedBuffer};
use crate::postings::compression::{BlockDecoder, VIntDecoder, COMPRESSION_BLOCK_SIZE};
use crate::postings::serializer::PostingsSerializer;
use crate::postings::BlockSearcher;
use crate::postings::FreqReadingOption;
use crate::postings::Postings;
use crate::postings::SkipReader;
use crate::postings::USE_SKIP_INFO_LIMIT;
use crate::schema::IndexRecordOption;
use crate::DocId;
use owned_read::OwnedRead;
use std::cmp::Ordering;
use tantivy_fst::Streamer;

struct PositionComputer {
    // store the amount of position int
    // before reading positions.
    //
    // if none, position are already loaded in
    // the positions vec.
    position_to_skip: usize,
    position_reader: PositionReader,
}

impl PositionComputer {
    pub fn new(position_reader: PositionReader) -> PositionComputer {
        PositionComputer {
            position_to_skip: 0,
            position_reader,
        }
    }

    pub fn add_skip(&mut self, num_skip: usize) {
        self.position_to_skip += num_skip;
    }

    // Positions can only be read once.
    pub fn positions_with_offset(&mut self, offset: u32, output: &mut [u32]) {
        self.position_reader.skip(self.position_to_skip);
        self.position_to_skip = 0;
        self.position_reader.read(output);
        let mut cum = offset;
        for output_mut in output.iter_mut() {
            cum += *output_mut;
            *output_mut = cum;
        }
    }
}

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings {
    block_cursor: BlockSegmentPostings,
    cur: usize,
    position_computer: Option<PositionComputer>,
    block_searcher: BlockSearcher,
}

impl SegmentPostings {
    /// Returns an empty segment postings object
    pub fn empty() -> Self {
        let empty_block_cursor = BlockSegmentPostings::empty();
        SegmentPostings {
            block_cursor: empty_block_cursor,
            cur: COMPRESSION_BLOCK_SIZE,
            position_computer: None,
            block_searcher: BlockSearcher::default(),
        }
    }

    /// Creates a segment postings object with the given documents
    /// and no frequency encoded.
    ///
    /// This method is mostly useful for unit tests.
    ///
    /// It serializes the doc ids using tantivy's codec
    /// and returns a `SegmentPostings` object that embeds a
    /// buffer with the serialized data.
    pub fn create_from_docs(docs: &[u32]) -> SegmentPostings {
        let mut buffer = Vec::new();
        {
            let mut postings_serializer = PostingsSerializer::new(&mut buffer, false, false);
            for &doc in docs {
                postings_serializer.write_doc(doc, 1u32);
            }
            postings_serializer
                .close_term(docs.len() as u32)
                .expect("In memory Serialization should never fail.");
        }
        let block_segment_postings = BlockSegmentPostings::from_data(
            docs.len() as u32,
            OwnedRead::new(buffer),
            IndexRecordOption::Basic,
            IndexRecordOption::Basic,
        );
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }
}

impl SegmentPostings {
    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub(crate) fn from_block_postings(
        segment_block_postings: BlockSegmentPostings,
        positions_stream_opt: Option<PositionReader>,
    ) -> SegmentPostings {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: COMPRESSION_BLOCK_SIZE, // cursor within the block
            position_computer: positions_stream_opt.map(PositionComputer::new),
            block_searcher: BlockSearcher::default(),
        }
    }
}

impl DocSet for SegmentPostings {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
//    #[inline]
    fn advance(&mut self) -> bool {
        if self.position_computer.is_some() && self.cur < COMPRESSION_BLOCK_SIZE {
            let term_freq = self.term_freq() as usize;
            if let Some(position_computer) = self.position_computer.as_mut() {
                position_computer.add_skip(term_freq);
            }
        }
        self.cur += 1;
        if self.cur >= self.block_cursor.block_len() {
            self.cur = 0;
            if !self.block_cursor.advance() {
                self.cur = COMPRESSION_BLOCK_SIZE;
                return false;
            }
        }
        true
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if !self.advance() {
            return SkipResult::End;
        }
        match self.doc().cmp(&target) {
            Ordering::Equal => {
                return SkipResult::Reached;
            }
            Ordering::Greater => {
                return SkipResult::OverStep;
            }
            _ => {
                // ...
            }
        }

        // In the following, thanks to the call to advance above,
        // we know that the position is not loaded and we need
        // to skip every doc_freq we cross.

        // skip blocks until one that might contain the target
        // check if we need to go to the next block
        let mut sum_freqs_skipped: u32 = 0;
        if !self
            .block_cursor
            .docs()
            .last()
            .map(|doc| *doc >= target)
            .unwrap_or(false)
        // there should always be at least a document in the block
        // since advance returned.
        {
            // we are not in the right block.
            //
            // First compute all of the freqs skipped from the current block.
            if self.position_computer.is_some() {
                sum_freqs_skipped = self.block_cursor.freqs()[self.cur..].iter().sum();
                match self.block_cursor.skip_to(target) {
                    BlockSegmentPostingsSkipResult::Success(block_skip_freqs) => {
                        sum_freqs_skipped += block_skip_freqs;
                    }
                    BlockSegmentPostingsSkipResult::Terminated => {
                        return SkipResult::End;
                    }
                }
            } else if self.block_cursor.skip_to(target)
                == BlockSegmentPostingsSkipResult::Terminated
            {
                // no positions needed. no need to sum freqs.
                return SkipResult::End;
            }
            self.cur = 0;
        }

        let cur = self.cur;

        // we're in the right block now, start with an exponential search
        let (output, len) = self.block_cursor.docs_aligned();
        let new_cur = self
            .block_searcher
            .search_in_block(&output, len, cur, target);
        if let Some(position_computer) = self.position_computer.as_mut() {
            sum_freqs_skipped += self.block_cursor.freqs()[cur..new_cur].iter().sum::<u32>();
            position_computer.add_skip(sum_freqs_skipped as usize);
        }
        self.cur = new_cur;

        // `doc` is now the first element >= `target`
        let doc = output.0[new_cur];
        debug_assert!(doc >= target);
        if doc == target {
            SkipResult::Reached
        } else {
            SkipResult::OverStep
        }
    }

    /// Return the current document's `DocId`.
    ///
    /// # Panics
    ///
    /// Will panics if called without having called advance before.
//    #[inline]
    fn doc(&self) -> DocId {
        let docs = self.block_cursor.docs();
        debug_assert!(
            self.cur < docs.len(),
            "Have you forgotten to call `.advance()` at least once before calling `.doc()`                                      ."
        );
        docs[self.cur]
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }

    fn append_to_bitset(&mut self, bitset: &mut BitSet) {
        // finish the current block
        if self.advance() {
            for &doc in &self.block_cursor.docs()[self.cur..] {
                bitset.insert(doc);
            }
            // ... iterate through the remaining blocks.
            while self.block_cursor.advance() {
                for &doc in self.block_cursor.docs() {
                    bitset.insert(doc);
                }
            }
        }
    }

    fn get_name(&mut self) -> &'static str {
        return "SegmentPostings";
    }
}

impl HasLen for SegmentPostings {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq()
    }
}

impl Postings for SegmentPostings {
    /// Returns the frequency associated to the current document.
    /// If the schema is set up so that no frequency have been encoded,
    /// this method should always return 1.
    ///
    /// # Panics
    ///
    /// Will panics if called without having called advance before.
    fn term_freq(&self) -> u32 {
        debug_assert!(
            // Here we do not use the len of `freqs()`
            // because it is actually ok to request for the freq of doc
            // even if no frequency were encoded for the field.
            //
            // In that case we hit the block just as if the frequency had been
            // decoded. The block is simply prefilled by the value 1.
            self.cur < COMPRESSION_BLOCK_SIZE,
            "Have you forgotten to call `.advance()` at least once before calling \
             `.term_freq()`."
        );
        self.block_cursor.freq(self.cur)
    }

    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let term_freq = self.term_freq() as usize;
        if let Some(position_comp) = self.position_computer.as_mut() {
            output.resize(term_freq, 0u32);
            position_comp.positions_with_offset(offset, &mut output[..]);
        } else {
            output.clear();
        }
    }
}

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
pub struct BlockSegmentPostings {
    doc_decoder: BlockDecoder,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,

    doc_freq: usize,
    doc_offset: DocId,

    num_vint_docs: usize,

    remaining_data: OwnedRead,
    skip_reader: SkipReader,
}

fn split_into_skips_and_postings(
    doc_freq: u32,
    mut data: OwnedRead,
) -> (Option<OwnedRead>, OwnedRead) {
    if doc_freq >= USE_SKIP_INFO_LIMIT {
        let skip_len = VInt::deserialize(&mut data).expect("Data corrupted").0 as usize;
        let mut postings_data = data.clone();
        postings_data.advance(skip_len);
        data.clip(skip_len);
        (Some(data), postings_data)
    } else {
        (None, data)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum BlockSegmentPostingsSkipResult {
    Terminated,
    Success(u32), //< number of term freqs to skip
}

impl BlockSegmentPostings {
    pub(crate) fn from_data(
        doc_freq: u32,
        data: OwnedRead,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> BlockSegmentPostings {
        let freq_reading_option = match (record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };

        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, data);
        let skip_reader = match skip_data_opt {
            Some(skip_data) => SkipReader::new(skip_data, record_option),
            None => SkipReader::new(OwnedRead::new(&[][..]), record_option),
        };
        let doc_freq = doc_freq as usize;
        let num_vint_docs = doc_freq % COMPRESSION_BLOCK_SIZE;
        BlockSegmentPostings {
            num_vint_docs,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            doc_offset: 0,
            doc_freq,
            remaining_data: postings_data,
            skip_reader,
        }
    }

    // Resets the block segment postings on another position
    // in the postings file.
    //
    // This is useful for enumerating through a list of terms,
    // and consuming the associated posting lists while avoiding
    // reallocating a `BlockSegmentPostings`.
    //
    // # Warning
    //
    // This does not reset the positions list.
    pub(crate) fn reset(&mut self, doc_freq: u32, postings_data: OwnedRead) {
        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, postings_data);
        let num_vint_docs = (doc_freq as usize) & (COMPRESSION_BLOCK_SIZE - 1);
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = postings_data;
        if let Some(skip_data) = skip_data_opt {
            self.skip_reader.reset(skip_data);
        } else {
            self.skip_reader.reset(OwnedRead::new(&[][..]))
        }
        self.doc_offset = 0;
        self.doc_freq = doc_freq as usize;
    }

    /// Returns the document frequency associated to this block postings.
    ///
    /// This `doc_freq` is simply the sum of the length of all of the blocks
    /// length, and it does not take in account deleted documents.
    pub fn doc_freq(&self) -> usize {
        self.doc_freq
    }

    /// Returns the array of docs in the current block.
    ///
    /// Before the first call to `.advance()`, the block
    /// returned by `.docs()` is empty.
    #[inline]
    pub fn docs(&self) -> &[DocId] {
        self.doc_decoder.output_array()
    }

    pub(crate) fn docs_aligned(&self) -> (&AlignedBuffer, usize) {
        self.doc_decoder.output_aligned()
    }

    /// Return the document at index `idx` of the block.
    #[inline]
    pub fn doc(&self, idx: usize) -> u32 {
        self.doc_decoder.output(idx)
    }

    /// Return the array of `term freq` in the block.
    #[inline]
    pub fn freqs(&self) -> &[u32] {
        self.freq_decoder.output_array()
    }

    /// Return the frequency at index `idx` of the block.
    #[inline]
    pub fn freq(&self, idx: usize) -> u32 {
        self.freq_decoder.output(idx)
    }

    /// Returns the length of the current block.
    ///
    /// All blocks have a length of `NUM_DOCS_PER_BLOCK`,
    /// except the last block that may have a length
    /// of any number between 1 and `NUM_DOCS_PER_BLOCK - 1`
    #[inline]
    fn block_len(&self) -> usize {
        self.doc_decoder.output_len
    }

    /// position on a block that may contains `doc_id`.
    /// Always advance the current block.
    ///
    /// Returns true if a block that has an element greater or equal to the target is found.
    /// Returning true does not guarantee that the smallest element of the block is smaller
    /// than the target. It only guarantees that the last element is greater or equal.
    ///
    /// Returns false iff all of the document remaining are smaller than
    /// `doc_id`. In that case, all of these document are consumed.
    ///
    pub fn skip_to(&mut self, target_doc: DocId) -> BlockSegmentPostingsSkipResult {
        let mut skip_freqs = 0u32;
        while self.skip_reader.advance() {
            if self.skip_reader.doc() >= target_doc {
                // the last document of the current block is larger
                // than the target.
                //
                // We found our block!
                let num_bits = self.skip_reader.doc_num_bits();
                let num_consumed_bytes = self.doc_decoder.uncompress_block_sorted(
                    self.remaining_data.as_ref(),
                    self.doc_offset,
                    num_bits,
                );
                self.remaining_data.advance(num_consumed_bytes);
                let tf_num_bits = self.skip_reader.tf_num_bits();
                match self.freq_reading_option {
                    FreqReadingOption::NoFreq => {}
                    FreqReadingOption::SkipFreq => {
                        let num_bytes_to_skip = compressed_block_size(tf_num_bits);
                        self.remaining_data.advance(num_bytes_to_skip);
                    }
                    FreqReadingOption::ReadFreq => {
                        let num_consumed_bytes = self
                            .freq_decoder
                            .uncompress_block_unsorted(self.remaining_data.as_ref(), tf_num_bits);
                        self.remaining_data.advance(num_consumed_bytes);
                    }
                }
                self.doc_offset = self.skip_reader.doc();
                return BlockSegmentPostingsSkipResult::Success(skip_freqs);
            } else {
                skip_freqs += self.skip_reader.tf_sum();
                let advance_len = self.skip_reader.total_block_len();
                self.doc_offset = self.skip_reader.doc();
                self.remaining_data.advance(advance_len);
            }
        }

        // we are now on the last, incomplete, variable encoded block.
        if self.num_vint_docs > 0 {
            let num_compressed_bytes = self.doc_decoder.uncompress_vint_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                self.num_vint_docs,
            );
            self.remaining_data.advance(num_compressed_bytes);
            match self.freq_reading_option {
                FreqReadingOption::NoFreq | FreqReadingOption::SkipFreq => {}
                FreqReadingOption::ReadFreq => {
                    self.freq_decoder
                        .uncompress_vint_unsorted(self.remaining_data.as_ref(), self.num_vint_docs);
                }
            }
            self.num_vint_docs = 0;
            return self
                .docs()
                .last()
                .map(|last_doc| {
                    if *last_doc >= target_doc {
                        BlockSegmentPostingsSkipResult::Success(skip_freqs)
                    } else {
                        BlockSegmentPostingsSkipResult::Terminated
                    }
                })
                .unwrap_or(BlockSegmentPostingsSkipResult::Terminated);
        }
        BlockSegmentPostingsSkipResult::Terminated
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) -> bool {
        if self.skip_reader.advance() {
            let num_bits = self.skip_reader.doc_num_bits();
            let num_consumed_bytes = self.doc_decoder.uncompress_block_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                num_bits,
            );
            self.remaining_data.advance(num_consumed_bytes);
            let tf_num_bits = self.skip_reader.tf_num_bits();
            match self.freq_reading_option {
                FreqReadingOption::NoFreq => {}
                FreqReadingOption::SkipFreq => {
                    let num_bytes_to_skip = compressed_block_size(tf_num_bits);
                    self.remaining_data.advance(num_bytes_to_skip);
                }
                FreqReadingOption::ReadFreq => {
                    let num_consumed_bytes = self
                        .freq_decoder
                        .uncompress_block_unsorted(self.remaining_data.as_ref(), tf_num_bits);
                    self.remaining_data.advance(num_consumed_bytes);
                }
            }
            // it will be used as the next offset.
            self.doc_offset = self.doc_decoder.output(COMPRESSION_BLOCK_SIZE - 1);
            true
        } else if self.num_vint_docs > 0 {
            let num_compressed_bytes = self.doc_decoder.uncompress_vint_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                self.num_vint_docs,
            );
            self.remaining_data.advance(num_compressed_bytes);
            match self.freq_reading_option {
                FreqReadingOption::NoFreq | FreqReadingOption::SkipFreq => {}
                FreqReadingOption::ReadFreq => {
                    self.freq_decoder
                        .uncompress_vint_unsorted(self.remaining_data.as_ref(), self.num_vint_docs);
                }
            }
            self.num_vint_docs = 0;
            true
        } else {
            false
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            num_vint_docs: 0,

            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,

            doc_offset: 0,
            doc_freq: 0,

            remaining_data: OwnedRead::new(vec![]),
            skip_reader: SkipReader::new(OwnedRead::new(vec![]), IndexRecordOption::Basic),
        }
    }
}

impl<'b> Streamer<'b> for BlockSegmentPostings {
    type Item = &'b [DocId];

    fn next(&'b mut self) -> Option<&'b [DocId]> {
        if self.advance() {
            Some(self.docs())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlockSegmentPostings;
    use super::BlockSegmentPostingsSkipResult;
    use super::SegmentPostings;
    use crate::common::HasLen;
    use crate::core::Index;
    use crate::docset::DocSet;
    use crate::postings::postings::Postings;
    use crate::schema::IndexRecordOption;
    use crate::schema::Schema;
    use crate::schema::Term;
    use crate::schema::INDEXED;
    use crate::DocId;
    use crate::SkipResult;
    use tantivy_fst::Streamer;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert!(!postings.advance());
        assert!(!postings.advance());
        assert_eq!(postings.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Have you forgotten to call `.advance()`")]
    fn test_panic_if_doc_called_before_advance() {
        SegmentPostings::empty().doc();
    }

    #[test]
    #[should_panic(expected = "Have you forgotten to call `.advance()`")]
    fn test_panic_if_freq_called_before_advance() {
        SegmentPostings::empty().term_freq();
    }

    #[test]
    fn test_empty_block_segment_postings() {
        let mut postings = BlockSegmentPostings::empty();
        assert!(!postings.advance());
        assert_eq!(postings.doc_freq(), 0);
    }

    #[test]
    fn test_block_segment_postings() {
        let mut block_segments = build_block_postings(&(0..100_000).collect::<Vec<u32>>());
        let mut offset: u32 = 0u32;
        // checking that the block before calling advance is empty
        assert!(block_segments.docs().is_empty());
        // checking that the `doc_freq` is correct
        assert_eq!(block_segments.doc_freq(), 100_000);
        while let Some(block) = block_segments.next() {
            for (i, doc) in block.iter().cloned().enumerate() {
                assert_eq!(offset + (i as u32), doc);
            }
            offset += block.len() as u32;
        }
    }

    #[test]
    fn test_skip_right_at_new_block() {
        let mut doc_ids = (0..128).collect::<Vec<u32>>();
        doc_ids.push(129);
        doc_ids.push(130);
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.skip_next(128), SkipResult::OverStep);
            assert_eq!(docset.doc(), 129);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 130);
            assert!(!docset.advance());
        }
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.skip_next(129), SkipResult::Reached);
            assert_eq!(docset.doc(), 129);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 130);
            assert!(!docset.advance());
        }
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.skip_next(131), SkipResult::End);
        }
    }

    fn build_block_postings(docs: &[DocId]) -> BlockSegmentPostings {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        let mut last_doc = 0u32;
        for &doc in docs {
            for _ in last_doc..doc {
                index_writer.add_document(doc!(int_field=>1u64));
            }
            index_writer.add_document(doc!(int_field=>0u64));
            last_doc = doc + 1;
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(int_field);
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = inverted_index.get_term_info(&term).unwrap();
        inverted_index.read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)
    }

    #[test]
    fn test_block_segment_postings_skip() {
        for i in 0..4 {
            let mut block_postings = build_block_postings(&[3]);
            assert_eq!(
                block_postings.skip_to(i),
                BlockSegmentPostingsSkipResult::Success(0u32)
            );
            assert_eq!(
                block_postings.skip_to(i),
                BlockSegmentPostingsSkipResult::Terminated
            );
        }
        let mut block_postings = build_block_postings(&[3]);
        assert_eq!(
            block_postings.skip_to(4u32),
            BlockSegmentPostingsSkipResult::Terminated
        );
    }

    #[test]
    fn test_block_segment_postings_skip2() {
        let mut docs = vec![0];
        for i in 0..1300 {
            docs.push((i * i / 100) + i);
        }
        let mut block_postings = build_block_postings(&docs[..]);
        for i in vec![0, 424, 10000] {
            assert_eq!(
                block_postings.skip_to(i),
                BlockSegmentPostingsSkipResult::Success(0u32)
            );
            let docs = block_postings.docs();
            assert!(docs[0] <= i);
            assert!(docs.last().cloned().unwrap_or(0u32) >= i);
        }
        assert_eq!(
            block_postings.skip_to(100_000),
            BlockSegmentPostingsSkipResult::Terminated
        );
        assert_eq!(
            block_postings.skip_to(101_000),
            BlockSegmentPostingsSkipResult::Terminated
        );
    }

    #[test]
    fn test_reset_block_segment_postings() {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        // create two postings list, one containg even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);

        let mut block_segments;
        {
            let term = Term::from_field_u64(int_field, 0u64);
            let inverted_index = segment_reader.inverted_index(int_field);
            let term_info = inverted_index.get_term_info(&term).unwrap();
            block_segments = inverted_index
                .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
        }
        assert!(block_segments.advance());
        assert_eq!(block_segments.docs(), &[0, 2, 4]);
        {
            let term = Term::from_field_u64(int_field, 1u64);
            let inverted_index = segment_reader.inverted_index(int_field);
            let term_info = inverted_index.get_term_info(&term).unwrap();
            inverted_index.reset_block_postings_from_terminfo(&term_info, &mut block_segments);
        }
        assert!(block_segments.advance());
        assert_eq!(block_segments.docs(), &[1, 3, 5]);
    }
}
