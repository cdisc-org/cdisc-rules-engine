CREATE TABLE IF NOT EXISTS public.data_metadata (
    -- general fields
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    -- general dataset metadata
    dataset_filename TEXT NOT NULL,
    dataset_filepath TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_hash TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    dataset_label TEXT,
    dataset_domain TEXT,
    -- handle supplementary dataset information
    dataset_is_supp BOOLEAN DEFAULT FALSE,
    dataset_rdomain TEXT,
    -- handle split dataset information
    dataset_is_split BOOLEAN DEFAULT FALSE,
    dataset_unsplit_name TEXT NOT NULL,
    dataset_split_part_number INTEGER,
    dataset_total_split_parts INTEGER,
    
    -- source tracking for concatenated datasets
    source_dataset_ids TEXT[], -- array of original dataset IDs that were concatenated
    concatenation_timestamp TIMESTAMPTZ, -- when concatenation occurred
    
    -- handle pre-processed dataset information
    dataset_preprocessed TIMESTAMPTZ,
    
    -- preprocessing fields
    contains_relrec_refs BOOLEAN DEFAULT FALSE,
    available_relrec_merges TEXT[],
    contains_co_refs BOOLEAN DEFAULT FALSE,
    contains_supp_refs BOOLEAN DEFAULT FALSE,
    preprocessing_stage TEXT DEFAULT 'raw',
    -- values: 'raw', 'split_processed', 'relrec_ready', 'relrec_merged', 'co_ready', 'co_merged', 'supp_ready', 'supp_merged'
    
    -- variable metadata
    var_name TEXT NOT NULL,
    var_label TEXT,
    var_type TEXT,
    var_length SMALLINT,
    var_format TEXT,
    
    last_preprocessing_run_id UUID, -- this links to preprocessing_results.run_id
    has_validation_errors BOOLEAN DEFAULT FALSE,
    validation_error_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_data_metadata_dataset_id ON public.data_metadata(dataset_id);
CREATE INDEX IF NOT EXISTS idx_data_metadata_dataset_name ON public.data_metadata(dataset_name);
CREATE INDEX IF NOT EXISTS idx_data_metadata_dataset_domain ON public.data_metadata(dataset_domain);
CREATE INDEX IF NOT EXISTS idx_data_metadata_var_name ON public.data_metadata(var_name);
CREATE INDEX IF NOT EXISTS idx_data_metadata_preprocessing_stage ON public.data_metadata(preprocessing_stage);
CREATE INDEX IF NOT EXISTS idx_data_metadata_contains_relrec ON public.data_metadata(contains_relrec_refs);
CREATE INDEX IF NOT EXISTS idx_data_metadata_is_split ON public.data_metadata(dataset_is_split);
CREATE INDEX IF NOT EXISTS idx_data_metadata_unsplit_name ON public.data_metadata(dataset_unsplit_name);
CREATE INDEX IF NOT EXISTS idx_data_metadata_preprocessed ON public.data_metadata(dataset_preprocessed);
CREATE INDEX IF NOT EXISTS idx_data_metadata_run_id ON public.data_metadata(last_preprocessing_run_id);
