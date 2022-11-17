use std::fs::File;
use std::cell::RefCell;
use std::rc::Rc;


pub struct Tree {
    path: &'static str,
    fd: Rc<RefCell<File>>,
}

