use std::str;
use chrono::NaiveDateTime;
use sanakirja::{Commit, Env, Error, MutTxn};
use sanakirja::btree;
use crate::{invalid_data_error, StoreRw, ID_SQ, DB_DUE_DATES, DB_LINKS, DB_NAMES, DB_RLINKS, DB_RSESSIONS, DB_SESSIONS};
use crate::reader::ChildIds;
use crate::types::{DueDate, RTriple, Session};

pub type StoreWriter<'env> = StoreRw<MutTxn<&'env Env, ()>>;

impl<'env> StoreWriter<'env> {
	pub fn add_child(&mut self, pid: u64, name: &str) -> Result<u64, Error> {
		assert_ne!(name.len(), 0);
		let next = self.get_child(pid)?.unwrap_or(0);

		let id = self.id;
		btree::del(&mut self.txn, &mut self.links, &pid, None)?;
		btree::put(&mut self.txn, &mut self.links, &pid, &id)?;

		btree::put(&mut self.txn, &mut self.rlinks, &id, &RTriple { pid, next, prev: 0 })?;
		if next > 0 { self.modify_rt(next, pid, |rt| rt.prev = id)? };

		btree::put(&mut self.txn, &mut self.names, &id, name.as_bytes())?;
		self.id += 1;
		Ok(id)
	}

	pub fn add_session(&mut self, id: u64, session: &Session) -> Result<(), Error> {
		btree::put(&mut self.txn, &mut self.sessions, &id, session)?;
		btree::put(&mut self.txn, &mut self.rsessions, session, &id)?;
		Ok(())
	}

	pub fn delete(&mut self, pid: u64, id: u64) -> Result<(), Error> {
		btree::del(&mut self.txn, &mut self.links, &pid, Some(&id))?;

		let rt = self.get_rt(id, pid)?.ok_or_else(invalid_data_error)?;
		if rt.prev > 0 { self.modify_rt(rt.prev, pid, |prt| prt.next = rt.next)?; }
		else if rt.next > 0 { btree::put(&mut self.txn, &mut self.links, &pid, &rt.next)?; }
		if rt.next > 0 { self.modify_rt(rt.next, pid, |nrt| nrt.prev = rt.prev)? };

		self.delete_helper(pid, id)
	}

	pub fn delete_session(&mut self, id: u64, session: &Session) -> Result<(), Error> {
		btree::del(&mut self.txn, &mut self.sessions, &id, Some(session))?;
		btree::del(&mut self.txn, &mut self.rsessions, session, Some(&id))?;
		Ok(())
	}

	pub fn rename(&mut self, id: u64, name: &str) -> Result<(), Error> {
		btree::del(&mut self.txn, &mut self.names, &id, None)?;
		btree::put(&mut self.txn, &mut self.names, &id, name.as_bytes())?;
		Ok(())
	}

	pub fn set_due_date(&mut self, id: u64, date: NaiveDateTime) -> Result<(), Error> {
		btree::del(&mut self.txn, &mut self.due_dates, &id, None)?;
		btree::put(&mut self.txn, &mut self.due_dates, &id, &DueDate(date))?;
		Ok(())
	}

	pub fn unset_due_date(&mut self, id: u64) -> Result<(), Error> {
		btree::del(&mut self.txn, &mut self.due_dates, &id, None)?;
		Ok(())
	}

	pub fn move_up(&mut self, pid: u64, id: u64) -> Result<(), Error> {
		let rt = self.get_rt(id, pid)?.ok_or_else(invalid_data_error)?;
		let prt = if rt.prev > 0 {
			self.get_rt(rt.prev, pid)?.ok_or_else(invalid_data_error)?
		} else { return Ok(()) };

		self.modify_rt(id, pid, |crt| {
			crt.next = rt.prev;
			crt.prev = prt.prev;
		})?;

		if rt.next > 0 {
			self.modify_rt(rt.next, pid, |nrt| nrt.prev = rt.prev)?;
		}

		self.modify_rt(rt.prev, pid, |prt| {
			prt.next = rt.next;
			prt.prev = id;
		})?;

		if prt.prev > 0 {
			self.modify_rt(prt.prev, pid, |pprt| pprt.next = id)?;
		} else {
			btree::del(&mut self.txn, &mut self.links, &pid, None)?;
			btree::put(&mut self.txn, &mut self.links, &pid, &id)?;
		}

		Ok(())
	}

	pub fn move_down(&mut self, pid: u64, id: u64) -> Result<(), Error> {
		let rt = self.get_rt(id, pid)?.ok_or_else(invalid_data_error)?;
		let nrt = if rt.next > 0 {
			self.get_rt(rt.next, pid)?.ok_or_else(invalid_data_error)?
		} else { return Ok(()) };

		self.modify_rt(id, pid, |crt| {
			crt.prev = rt.next;
			crt.next = nrt.next;
		})?;

		if rt.prev > 0 {
			self.modify_rt(rt.prev, pid, |prt| prt.next = rt.next)?;
		} else {
			btree::del(&mut self.txn, &mut self.links, &pid, None)?;
			btree::put(&mut self.txn, &mut self.links, &pid, &rt.next)?;
		}

		self.modify_rt(rt.next, pid, |nrt| {
			nrt.prev = rt.prev;
			nrt.next = id;
		})?;

		if nrt.next > 0 {
			self.modify_rt(nrt.next, pid, |nnrt| nnrt.prev = id)?;
		}
		Ok(())
	}

	pub fn share(&mut self, src: u64, dest: u64) -> Result<bool, Error> {
		if self.is_descendent_of(dest, src)? { return Ok(false) };
		if self.get_rt(src, dest)?.is_some() { return Ok(false) };

		let next = self.get_child(dest)?.unwrap_or(0);

		btree::del(&mut self.txn, &mut self.links, &dest, None)?;
		btree::put(&mut self.txn, &mut self.links, &dest, &src)?;

		btree::put(&mut self.txn, &mut self.rlinks, &src, &RTriple { pid: dest, next, prev: 0 })?;
		if next > 0 { self.modify_rt(next, dest, |rt| rt.prev = src)? };

		Ok(true)
	}

	pub fn cut(&mut self, src_pid: u64, src: u64, dest: u64) -> Result<bool, Error> {
		if !self.share(src, dest)? { return Ok(false) };

		let is_head = btree::del(&mut self.txn, &mut self.links, &src_pid, Some(&src))?;

		let rt = self.get_rt(src, src_pid)?.ok_or_else(invalid_data_error)?;
		if is_head { btree::put(&mut self.txn, &mut self.links, &src_pid, &rt.next)?; }
		if rt.prev > 0 { self.modify_rt(rt.prev, src_pid, |prt| prt.next = rt.next)? };
		if rt.next > 0 { self.modify_rt(rt.next, src_pid, |nrt| nrt.prev = rt.prev)? };
		btree::del(&mut self.txn, &mut self.rlinks, &src, Some(&rt))?;

		Ok(true)
	}

	pub fn commit(mut self) -> Result<(), Error> {
		self.txn.set_root(ID_SQ, self.id);
		self.txn.set_root(DB_LINKS, self.links.db.into());
		self.txn.set_root(DB_RLINKS, self.rlinks.db.into());
		self.txn.set_root(DB_NAMES, self.names.db.into());
		self.txn.set_root(DB_DUE_DATES, self.due_dates.db.into());
		self.txn.set_root(DB_SESSIONS, self.sessions.db.into());
		self.txn.set_root(DB_RSESSIONS, self.rsessions.db.into());
		self.txn.commit()
	}

	fn modify_rt(&mut self, id: u64, pid: u64, f: impl Fn(&mut RTriple)) -> Result<(), Error> {
		let mut rt = self.get_rt(id, pid)?.ok_or_else(invalid_data_error)?;
		btree::del(&mut self.txn, &mut self.rlinks, &id, Some(&rt))?;
		f(&mut rt);
		btree::put(&mut self.txn, &mut self.rlinks, &id, &rt)?;
		Ok(())
	}

	fn delete_helper(&mut self, pid: u64, id: u64) -> Result<(), Error> {
		let rt = self.get_rt(id, pid)?.ok_or_else(invalid_data_error)?;
		btree::del(&mut self.txn, &mut self.rlinks, &id, Some(&rt))?;

		if let Some((&eid, _)) = btree::get(&self.txn, &self.rlinks, &id, None)? {
			if eid == id { return Ok(()) };
		}

		btree::del(&mut self.txn, &mut self.names, &id, None)?;
		btree::del(&mut self.txn, &mut self.due_dates, &id, None)?;
		self.delete_id_sessions(id)?;

		let mut child_ids = ChildIds::new(self, id)?;
		if let Some(first) = child_ids.next(self)? {
			btree::del(&mut self.txn, &mut self.links, &id, Some(&first))?;
			self.delete(id, first)?;
		}
		while let Some(child_id) = child_ids.next(self)? {
			self.delete(id, child_id)?;
		}
		Ok(())
	}

	fn is_descendent_of(&self, subj: u64, pred: u64) -> Result<bool, Error> {
		if subj == pred { return Ok(true) };
		for child_id in self.child_ids(pred)? {
			if self.is_descendent_of(subj, child_id?)? { return Ok(true) };
		}
		Ok(false)
	}

	fn delete_id_sessions(&mut self, id: u64) -> Result<(), Error> {
		while let Some((&eid, &session)) = btree::get(&self.txn, &self.sessions, &id, None)? {
			if eid != id { break };
			self.delete_session(id, &session)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
	use sanakirja::{Env, Error};
	use crate::Store;
	use crate::types::Session;

	#[test]
	fn add_child() -> Result<(), Error> {
		let store = Store { env: Env::new_anon(1 << 14, 2)? };
		store.create_base()?;

		let mut writer = store.writer()?;
		writer.add_child(0, "first")?;
		writer.add_child(1, "second")?;
		writer.commit()?;

		let reader = store.reader()?;
		let mut iter = reader.child_ids(0)?;

		let id = iter.next().unwrap()?;
		let data = reader.name(id)?.unwrap();
		assert_eq!(id, 1);
		assert_eq!(data, "first");
		assert!(iter.next().is_none());

		let mut iter = reader.child_ids(1)?;
		let id = iter.next().unwrap()?;
		let data = reader.name(id)?.unwrap();
		assert_eq!(id, 2);
		assert_eq!(data, "second");
		assert!(iter.next().is_none());

		Ok(())
	}

	#[test]
	fn add_session() -> Result<(), Error> {
		let store = Store { env: Env::new_anon(1 << 14, 2)? };
		store.create_base()?;

		let mut writer = store.writer()?;
		let start = NaiveDateTime::new(
			NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
			NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
		);
		let end = NaiveDateTime::new(
			NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
			NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
		);
		let session = Session { start, end };
		writer.add_session(0, &session)?;
		writer.commit()?;

		let reader = store.reader()?;
		assert_eq!(reader.first_session(0)?.unwrap(), session);

		Ok(())
	}
}
