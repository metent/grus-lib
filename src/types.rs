#[cfg(feature = "interim")]
use std::str::FromStr;
use chrono::NaiveDateTime;
#[cfg(feature = "interim")]
use chrono::Local;
#[cfg(feature = "interim")]
use interim::{parse_date_string, DateError, Dialect};
use sanakirja::Storable;
use sanakirja::btree::{Db, UDb};

#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
pub struct Session {
	pub start: NaiveDateTime,
	pub end: NaiveDateTime,
}

impl Storable for Session {
	type PageReferences = core::iter::Empty<u64>;
	fn page_references(&self) -> Self::PageReferences {
		core::iter::empty()
	}

	fn compare<T>(&self, _: &T, b: &Self) -> core::cmp::Ordering {
		self.cmp(b)
	}
}

#[cfg(feature = "interim")]
impl FromStr for Session {
	type Err = DateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.split_once(" to ") {
			Some((s, e)) => Ok(Session {
				start: parse_date_string(s, Local::now(), Dialect::Uk)?.naive_local(),
				end: parse_date_string(e, Local::now(), Dialect::Uk)?.naive_local(),
			}),
			None => Err(DateError::MissingDate),
		}
	}
}

pub(crate) type LinksDb = Db<u64, u64>;
pub(crate) type RLinksDb = Db<u64, RTriple>;
pub(crate) type NamesDb = UDb<u64, [u8]>;
pub(crate) type DueDatesDb = Db<u64, DueDate>;
pub(crate) type SessionsDb = Db<u64, Session>;
pub(crate) type RSessionsDb = Db<Session, u64>;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub(crate) struct RTriple {
	pub pid: u64,
	pub next: u64,
	pub prev: u64,
}

impl Storable for RTriple {
	type PageReferences = core::iter::Empty<u64>;
	fn page_references(&self) -> Self::PageReferences {
		core::iter::empty()
	}

	fn compare<T>(&self, _: &T, b: &Self) -> core::cmp::Ordering {
		self.cmp(b)
	}
}

#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct DueDate(pub NaiveDateTime);

impl Storable for DueDate {
	type PageReferences = core::iter::Empty<u64>;
	fn page_references(&self) -> Self::PageReferences {
		core::iter::empty()
	}

	fn compare<T>(&self, _: &T, b: &Self) -> core::cmp::Ordering {
		self.cmp(b)
	}
}
