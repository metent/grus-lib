pub mod reader;
pub mod types;
pub mod writer;

pub use sanakirja::Error;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use sanakirja::{Commit, Env, LoadPage, RootDb};
use sanakirja::btree;
use reader::StoreReader;
use types::{DueDatesDb, LinksDb, NamesDb, RLinksDb, RSessionsDb, SessionsDb};
use writer::StoreWriter;

const ID_SQ: usize = 0;
const DB_LINKS: usize = 1;
const DB_RLINKS: usize = 2;
const DB_NAMES: usize = 3;
const DB_DUE_DATES: usize = 4;
const DB_SESSIONS: usize = 5;
const DB_RSESSIONS: usize = 6;

pub struct Store {
	env: Env
}

impl Store {
	pub fn open<P: AsRef<Path>>(path: P, n_roots: usize) -> Result<Self, Error> {
		let store = Store { env: Env::new(path, 1 << 14, n_roots)? };
		store.create_base()?;
		Ok(store)
	}

	pub fn reader(&self) -> Result<StoreReader, Error> {
		let txn = Env::txn_begin(&self.env)?;
		let id = txn.root(ID_SQ);
		let links = txn.root_db(DB_LINKS).ok_or_else(invalid_data_error)?;
		let rlinks = txn.root_db(DB_RLINKS).ok_or_else(invalid_data_error)?;
		let names = txn.root_db(DB_NAMES).ok_or_else(invalid_data_error)?;
		let due_dates = txn.root_db(DB_DUE_DATES).ok_or_else(invalid_data_error)?;
		let sessions = txn.root_db(DB_SESSIONS).ok_or_else(invalid_data_error)?;
		let rsessions = txn.root_db(DB_RSESSIONS).ok_or_else(invalid_data_error)?;
		Ok(StoreReader { txn, id, links, rlinks, names, due_dates, sessions, rsessions })
	}

	pub fn writer(&self) -> Result<StoreWriter, Error> {
		let txn = Env::mut_txn_begin(&self.env)?;
		let id = txn.root(ID_SQ).ok_or_else(invalid_data_error)?;
		let links = txn.root_db(DB_LINKS).ok_or_else(invalid_data_error)?;
		let rlinks = txn.root_db(DB_RLINKS).ok_or_else(invalid_data_error)?;
		let names = txn.root_db(DB_NAMES).ok_or_else(invalid_data_error)?;
		let due_dates = txn.root_db(DB_DUE_DATES).ok_or_else(invalid_data_error)?;
		let sessions = txn.root_db(DB_SESSIONS).ok_or_else(invalid_data_error)?;
		let rsessions = txn.root_db(DB_RSESSIONS).ok_or_else(invalid_data_error)?;
		Ok(StoreWriter { txn, id, links, rlinks, names, due_dates, sessions, rsessions })
	}

	fn create_base(&self) -> Result<(), Error> {
		let mut txn = Env::mut_txn_begin(&self.env)?;

		let id = txn.root(ID_SQ);
		let links: Option<LinksDb> = txn.root_db(DB_LINKS);
		let rlinks: Option<RLinksDb> = txn.root_db(DB_RLINKS);
		let names: Option<NamesDb> = txn.root_db(DB_NAMES);
		let due_dates: Option<DueDatesDb> = txn.root_db(DB_DUE_DATES);
		let sessions: Option<SessionsDb> = txn.root_db(DB_SESSIONS);
		let rsessions: Option<RSessionsDb> = txn.root_db(DB_RSESSIONS);
		match (id, links, rlinks, names, due_dates, sessions, rsessions) {
			(Some(_), Some(_), Some(_), Some(_), Some(_), Some(_), Some(_)) => Ok(()),
			(None, None, None, None, None, None, None) => {
				unsafe {
					let links: LinksDb = btree::create_db(&mut txn)?;
					let rlinks: RLinksDb = btree::create_db(&mut txn)?;
					let mut names: NamesDb = btree::create_db_(&mut txn)?;
					let due_dates: DueDatesDb = btree::create_db(&mut txn)?;
					let sessions: SessionsDb = btree::create_db(&mut txn)?;
					let rsessions: RSessionsDb = btree::create_db(&mut txn)?;

					btree::put(&mut txn, &mut names, &0, b"/")?;

					txn.set_root(ID_SQ, 1);
					txn.set_root(DB_LINKS, links.db.into());
					txn.set_root(DB_RLINKS, rlinks.db.into());
					txn.set_root(DB_NAMES, names.db.into());
					txn.set_root(DB_DUE_DATES, due_dates.db.into());
					txn.set_root(DB_SESSIONS, sessions.db.into());
					txn.set_root(DB_RSESSIONS, rsessions.db.into());
				}
				txn.commit()
			}
			_ => {
				Err(invalid_data_error())
			}
		}
	}
}


pub struct StoreRw<T: LoadPage> {
	txn: T,
	id: u64,
	links: LinksDb,
	rlinks: RLinksDb,
	names: NamesDb,
	due_dates: DueDatesDb,
	sessions: SessionsDb,
	rsessions: RSessionsDb,
}

fn invalid_data_error() -> Error {
	Error::IO(IoError::new(
		ErrorKind::InvalidData,
		"Database is invalid or corrupted"
	))
}
