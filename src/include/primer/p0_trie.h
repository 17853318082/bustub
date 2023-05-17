//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    key_char_ = key_char;    // 初始化当前节点的值
    is_end_ = false;          // 当前节点不为最后一个
    children_.clear();       // 当前节点无孩子节点
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_ = std::move(other_trie_node.children_);    // 要用移动语义对其进行赋值
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { 
    if(this->children_.find(key_char)!=this->children_.end()){
      return true;
    }else{
      return false;
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !this->children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return this->is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return this->key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if(HasChild(key_char)||key_char!=child->key_char_){
      return nullptr;
    }else{
      this->children_[key_char] = std::forward<std::unique_ptr<TrieNode>>(child);   // forward:是一个模板函数，用于实现完美转发
      return &this->children_[key_char];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) { 
    if(HasChild(key_char)){
      return &(this->children_[key_char]);    // 返回子节点指针
    }else{
      return nullptr;
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {this->children_.erase(key_char);}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) {this->is_end_ = is_end;}

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value):TrieNode(std::forward<TrieNode>(trieNode)) {
    value_ = value;
    SetEndNode(true);         // 将该节点设置为结束节点
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value):TrieNode(key_char) {
    value_ = value;     // 该节点的value值
    SetEndNode(true);
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>(TrieNode('\0')); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
   /*插入节点*/
  template <typename T>
  bool Insert(const std::string &key, T value) {
    // 如果值为空，则返回false
    if(key.empty()){
      return false;
    }
    latch_.WLock();   // 对树进行加锁
    auto cur_node = &root_; // 获得根节点，根节点中不存值
    // 当值不为空时，我们开始进行节点的插入
    for(size_t i=0;i<key.size();i++){

      // 查看当前节点的孩子是否有当前字符,如果有，则进入孩子节点，如果没有则新建一个节点
      if(cur_node->get()->HasChild(key[i])){
        cur_node = cur_node->get()->GetChildNode(key[i]);   // 进入子节点
      }else{
        // 将其插入到子节点中
        cur_node = cur_node->get()->InsertChildNode(key[i],std::make_unique<TrieNode>(key[i])); // 插入一个新节点,并进入子节点
      }

      // 此时说明，当前值已经是最后一个值了，查看，当前节点是否是尾节点
      if(i==key.size()-1){
        // 如果当前节点是尾节点，说明已经存在值了 ,条件3
        if(cur_node->get()->IsEndNode()){
          latch_.WUnlock();
          return false;
        }
        // 如果当前节点不是尾节点，则将当前节点添加为尾节点并返回插入成功：条件1，2
        auto new_node = new TrieNodeWithValue(std::move(**cur_node),value);   // 如果是最后一个则构造一个有值节点，将值存入节点，加入到树中
        cur_node->reset(new_node);    // 将该节点的unique_ptr存入新节点的指针
        latch_.WUnlock();            // 对树解锁，操作结束
        return true;                 // 到此节点插入完毕，返回true
      }
    }
    return false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given ke y.                                 找到给定值的终结点
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.                                                       如果终结点没有孩子节点，则从父节点的孩子节点中将其移除，即将尾节点置为，非尾节点
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.                                                               递归移除没有孩子的节点并且不是终结点的节点
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */

  // 递归删除
  bool Remove(const std::string &key) { 
    latch_.WLock();    // 加锁
    bool a = true;
    bool* success = &a;     // 标记是否删除成功
    Dfs(key,&root_,0,success);   // 递归删除
    latch_.WUnlock();
    return *success; 
  }
  // 递归删除节点，size_t为无符号整形
  auto Dfs(const std::string &key,std::unique_ptr<TrieNode>*root,size_t index,bool *success)->std::unique_ptr<TrieNode>*{
    // 到达节点尾部
    if(index == key.size()){
      // 当前节点不是尾节点
      if(!root->get()->IsEndNode()){
        *success = false;
        return nullptr;
      }
      // 是尾节点，查看是否有子节点，没有子节点的情况
      if(!root->get()->HasChildren()){
        *success = true;
        return nullptr;
      }
      // 是尾节点且没有子节点的情况下，对其进行删除-----非物理删除----将尾节点置为非尾节点
      *success = true;
      root->get()->SetEndNode(false);
      return root;
    }
    // 未到达尾节点  
    std::unique_ptr<TrieNode>* node;
    if(root->get()->HasChild(key[index])){
      // 继续向下找
      node = Dfs(key,root->get()->GetChildNode(key[index]),index+1,success);
      // 未删除成功
      if(*success==false){
        return nullptr;
      }
      // 删除成功，当没有子节点的时候，将其进行物理删除
      if(node == nullptr){
        root->get()->RemoveChildNode(key[index]);
        // 继续进行删除
        if(!root->get()->HasChildren()&&!root->get()->IsEndNode()){
          return nullptr;
        }
      }
      return root;
    }
    // 节点为空的情况
    *success = false;
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    // 查询为空时，查询失败，并直接返回
    if(key.empty()){
      *success = false;
      return {};
    }
    *success = false;
    auto cur_node = &root_; // 获取根节点
    latch_.RLock();         // 加读锁
    // 遍历查询该值
    for(size_t i=0;i<key.size();i++){
      // 如果当前节点存在于子节点中,那么进入子节点,否则返回false，说明该未查找成功
      if(cur_node->get()->HasChild(key[i])){
        cur_node = cur_node->get()->GetChildNode(key[i]);
      }else{
        latch_.RUnlock();     // 解锁
        return {};
      }

      // 找到了最后一个元素，并且当前节点为终结点，那么返回当前节点的值，并将success设置为true
      if(i==key.size()-1){
        // 当前节点为终节点，仅有这一种情况为查找成功
        if(cur_node->get()->IsEndNode()){
          auto flag_node = dynamic_cast<TrieNodeWithValue<T> *>(cur_node->get());
          *success = true;
          latch_.RUnlock();               // 解锁
          return flag_node->GetValue();
        }else{ 
          // 当前节点不为终结点,没有值，返回
          latch_.RUnlock();
          return {};
        }
      }
    }
    latch_.RLock();    //未进入for循环，解锁
    return {};
  }
};
}  // namespace bustub
