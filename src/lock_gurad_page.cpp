//
// Created by Yicheng Zhang on 12/13/23.
//
#include "../include/eth_storage/lock_gurad_page.h"
#include "../include/eth_storage/page.h"

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept
    -> BasicPageGuard & {
  if (this != &that) {
    // If the current page is being guarded, unpin it
    if (bpm_ != nullptr && page_ != nullptr) {
      bpm_->UnpinPage(PageId(), is_dirty_);
    }
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }; // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard rpg(bpm_, page_);
  page_->RLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard wpg(bpm_, page_);
  page_->WLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  return wpg;
}
// does not need to Latch the page in read mode because it is already latches
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {}

// does not need to Latch the page in read mode because it is already latches
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept
    -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); } // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {}
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept
    -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); } // NOLINT
