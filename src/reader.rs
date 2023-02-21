use std::str;
use chrono::NaiveDateTime;
use sanakirja::{Env, Error, LoadPage, Txn};
use sanakirja::btree;
use super::{invalid_data_error, StoreRw};
use super::types::{RTriple, Session};

pub type StoreReader<'env> = StoreRw<Txn<&'env Env>>;

impl<T: LoadPage<Error = Error>> StoreRw<T> {
	pub fn name(&self, id: u64) -> Result<Option<&str>, Error> {
		match btree::get(&self.txn, &self.names, &id, None)? {
			Some((&eid, name)) if eid == id => Ok(Some(str::from_utf8(name).map_err(|_| invalid_data_error())?)),
			_ => Ok(None),
		}
	}

	pub fn due_date(&self, id: u64) -> Result<Option<NaiveDateTime>, Error> {
		match btree::get(&self.txn, &self.due_dates, &id, None)? {
			Some((&eid, due_date)) if eid == id => Ok(Some(due_date.0)),
			_ => Ok(None),
		}
	}

	pub fn first_session(&self, id: u64) -> Result<Option<Session>, Error> {
		match btree::get(&self.txn, &self.sessions, &id, None)? {
			Some((&eid, &session)) if eid == id => Ok(Some(session)),
			_ => Ok(None)
		}
	}

	pub fn child_ids(&self, id: u64) -> Result<ChildIdIter<'_, T>, Error> {
		Ok(ChildIdIter {
			reader: self,
			child_ids: ChildIds::new(self, id)?,
		})
	}

	pub fn sessions<'r>(&'r self, id: u64) -> Result<impl Iterator<Item = Result<(&'r u64, &'r Session), Error>>, Error> {
		let iter = btree::iter(&self.txn, &self.sessions, Some((&id, None)))?;
		Ok(iter.take_while(move |entry| match entry {
			Ok((&eid, _)) if eid > id => false,
			_ => true,
		}))
	}

	pub fn all_names<'r>(&'r self) -> Result<impl Iterator<Item = Result<(&'r u64, &'r str), Error>>, Error> {
		Ok(
			btree::iter(&self.txn, &self.names, None)?
			.map(|item| item.and_then(|(id, name)| Ok((
				id, str::from_utf8(name).map_err(|_| invalid_data_error())?
			))))
		)
	}

	pub fn all_sessions<'s>(&'s self) -> Result<impl Iterator<Item = Result<(&'s Session, &'s u64), Error>>, Error> {
		btree::iter(&self.txn, &self.rsessions, None)
	}

	pub(crate) fn get_child(&self, id: u64) -> Result<Option<u64>, Error> {
		match btree::get(&self.txn, &self.links, &id, None)? {
			Some((&eid, &child)) if eid == id => Ok(Some(child)),
			_ => Ok(None)
		}
	}

	pub(crate) fn get_rt(&self, id: u64, pid: u64) -> Result<Option<RTriple>, Error> {
		match btree::get(&self.txn, &self.rlinks, &id, Some(&RTriple { pid, next: 0, prev: 0 }))? {
			Some((&eid, &rt)) if eid == id && rt.pid == pid => Ok(Some(rt)),
			_ => Ok(None)
		}
	}
}

pub struct ChildIdIter<'reader, T: LoadPage<Error = Error>> {
	reader: &'reader StoreRw<T>,
	child_ids: ChildIds,
}

impl<'reader, T: LoadPage<Error = Error>> Iterator for ChildIdIter<'reader, T> {
	type Item = Result<u64, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		self.child_ids.next(self.reader).transpose()
	}
}

pub(crate) struct ChildIds {
	pid: u64,
	id: u64,
}

impl ChildIds {
	pub(crate) fn new<T: LoadPage<Error = Error>>(reader: &StoreRw<T>, id: u64) -> Result<Self, Error> {
		Ok(ChildIds { pid: id, id: reader.get_child(id)?.unwrap_or(0) })
	}

	pub(crate) fn next<T: LoadPage<Error = Error>>(&mut self, reader: &StoreRw<T>) -> Result<Option<u64>, Error> {
		let id = self.id;
		if id == 0 { return Ok(None) };
		let RTriple { next, .. } = reader.get_rt(id, self.pid)?.ok_or_else(invalid_data_error)?;
		self.id = next;
		Ok(Some(id))
	}
}
