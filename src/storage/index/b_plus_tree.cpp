#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty b+树是否为空
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key 返回一个与输入key相关的value
 * This method is used for point query
 * @return : true means key exists
 */

/**
 * 根据key查找Value在B+树中
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 对根节点加锁
  root_page_id_latch_.RLock();
  // 查找存储数据的叶子节点
  auto leaf_page = FindLeaf(key, Operation::SEARCH, transaction);
  // 将获取的叶子节点进行强转
  auto *node = reinterpret_cast<LeafPage *>(leaf_page);

  ValueType v;
  // 从叶子节点中获取key所对应的页面,找到之后将值存入v
  auto exited = node->Lookup(key, &v, comparator_);

  // 如果不存在，则返回false
  if (!exited) {
    return false;
  }
  // 如果存在，将页面加入结果集
  result->push_back(v);
  return true;
}

/**
 * 查找key所在的叶子节点，如果存在
 * params:
 * 1. key的类型
 * 2. 操作：Search，Insert，Remove
 * 3. 事务锁
 * script：
 *
 * return：
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost,
                              bool rightMost) -> Page * {
  // assert() 是一个预处理宏，用于在程序运行时检查指定的条件是否为真。如果条件为假，assert()
  // 将终止程序的执行，并向标准错误流输出一条错误消息。
  assert(operation == Operation::SEARCH ? !(leftMost && rightMost) : transaction != nullptr);
  // 查看树是否为空
  assert(root_page_id_ != INVALID_PAGE_ID);
  // 从缓存池中根页
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  // 根据页面id，获取页面数据
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // 如果操作为查询，则解开根锁
  if (operation == Operation::SEARCH) {
    root_page_id_latch_.RUnlock();
    page->RLatch();  // 对当前页面加读锁
  } else {
    page->WLatch();
  }
  // 从页面循环查询寻子节点,直至查询到叶子节点时结束
  while (!node->IsLeafPage()) {
    // 查询key所在的子节点
    auto *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    // 查询子节点,如果是最左侧节点，直接返回最左侧节点
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);  // 从中间查找叶子节点页面id
    }

    // 获取子节点的page，和node
    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);  // 获取一个新页
    auto child_node = reinterpret_cast<BPlusTreePage *>(
        child_page->GetData());  // 这里应该先将页面转换为BPlusTreePage，然后判断该节点是不是叶子页

    // 如果当前是查询操作，则解锁当前页面,加锁子页面
    if (operation == Operation::SEARCH) {
      child_page->RLatch();                                       // 加锁子页面
      page->RUnlatch();                                           // 解锁当前页面
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 取消固定当前页面
    } else {
      child_page->WLatch();
    }
    // 进入下一个页面
    page = child_page;
    node = child_node;
  }
  return page;  // 查找到key所在的叶子页，返回
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree 插入键值对到b+树中
 * if current tree is empty, start new tree, update root page id and insert
 * 如果当前树为空，则新建一个树，更新root_page_id，并且插入到insert中 entry, otherwise insert into leaf page.
 * 否则插入到叶子页面中
 * @return: since we only support unique key, if user try to insert duplicate 仅支持插入唯一的key
 * keys return false, otherwise return true.  如果尝试插入相同key则返回false，否则插入成功返回true
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 首先判断，当前树是否为空,如果为空则新建一个树，如果不为空则执行插入
  if (IsEmpty()) {
    // 创建一颗新树，即当前根节点本身就是一个叶子节点
    CreateNewTree(key, value);
    return true;
  }
  // 不为空时，执行插入
  return InsertIntoLeaf(key, value, transaction);
}
/**
 * 创建一颗新树
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateNewTree(const KeyType &key, const ValueType &value) {
  // 开始创建新树
  auto new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  // 无法创建页面，没有内存了
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Connot allocate new pgae");
  }
  // 创建成功的情况下,创建一个叶子page,并将数据存入该页
  auto *leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  // 初始化页面
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  // 将值插入到叶子节点中
  leaf->Insert(key, value, comparator_);
  // 将页面加速缓存池
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
}

/**
 * 将键值对插入到叶子节点中
 * 1. 首先找到要插入节点的位置：leaf_page
 * 2. 根据插入位置
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 查找可进行插入的叶子节点
  auto leaf_page = FindLeaf(key, Operation::INSERT, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  auto size = node->GetSize();
  auto new_size = node->Insert(key, value, comparator_);  // 执行插入

  // 节点中含有该key，则插入失败
  if (new_size == size) {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // 从缓冲池中取消锁定
    return false;
  }
  // 叶子不满的情况，返回true
  if (new_size < leaf_max_size_) {
    leaf_page->WUnlatch();  // 解锁
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }
  // 叶子满的情况，执行节点分裂
  auto sibling_leaf_node = Split(node);  // 分裂当前节点
  sibling_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(sibling_leaf_node->GetPageId());

  // 将新分裂出的新节点插入到父结点中
  auto rise_key = sibling_leaf_node->KeyAt(0);  // 将第一个位置的key，存入父节点
  InsertIntoParent(node, rise_key, sibling_leaf_node, transaction);

  leaf_page->WUnlatch();  // 解锁叶子节点
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
  return true;
}

/**
 * 执行节点分裂，创建一个新节点，将就节点一般的内容复制到新节点，并返回新节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 新建一个页面
  page_id_t page_id;
  // 在缓冲池中新建一个页面
  auto page = buffer_pool_manager_->NewPage(&page_id);
  // 新建失败
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Connot allocate new page");
  }
  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->SetPageType(node->GetPageType());  // 新的pagetype与旧页相同
  // 如果当前需要分裂的节点是叶子节点
  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);                                 // 原叶子节点
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);                         // 新建叶子节点
    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);  //  初始化叶子节点
    leaf->MoveHalfTo(new_leaf);  // 移动一般数据到新的节点上
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }
  return new_node;
}

/**
 * 将节点插入到父结点中，递归插入父节点，直至节点不再分裂为止
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 如果分裂节点为根节点
  if (old_node->IsRootPage()) {
    // 新建页面
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Connot allocate new page");
    }
    // 新建根节点
    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);
    // 将新旧节点，插入到新建的根节点
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 设置两个节点的父节点为根节点page_id
    old_node->SetParentPageId(page->GetPageId());
    new_node->SetParentPageId(page->GetPageId());

    // 取消固定根节点页面
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    // 更新根节点记录
    UpdateRootPageId(0);
    return;
  }
  // 当分裂节点不为根节点时，查看插入父节点后，父节点是否满足分裂条件，如果满足则继续分裂，否则终止递归，插入结束
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // 获取公共父节点id
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 查看父节点是否已满,如果未满，则直接插入
  if (parent_node->GetSize() < internal_max_size_) {
    // 将新节点插入到父节点尾部
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // 父节点从缓冲池中取消固定
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }
  // 父节点已满，对父节点执行分裂操作
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)];
  auto *copy_parent_node = reinterpret_cast<InternalPage *>(mem);
  // 将父节点数据部分，复制到数组mem
  std::memcpy(mem, parent_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * parent_node->GetSize());
  copy_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // 将新节点插入到拷贝的父结点中
  auto parent_new_sibling_node = Split(copy_parent_node);  // 将拷贝节点分裂，产生新的节点
  auto new_key = parent_new_sibling_node->KeyAt(0);
  std::memcpy(parent_page->GetData(), mem,
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_parent_node->GetMinSize());
  InsertIntoParent(parent_node, new_key, parent_new_sibling_node, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
  // 删除拷贝数组
  delete[] mem;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key 删除与输入key相关的键值对
 * If current tree is empty, return immdiately.  如果树是空的则立刻返回
 * If not, User needs to first find the right leaf page as deletion target, then 如果不为空，则用户首先需要发现对的leaf
 * page作为删除目标 delete entry from leaf page. Remember to deal with redistribute or merge if  那么从leaf
 * page中删除，如果有需要的话需要重新分配合并 necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 对根节点进行加锁
  root_page_id_latch_.WLock();
  // 查看树是否为空
  if (IsEmpty()) {
    return;
  }
  // 查找key所在的叶子节点
  auto leaf_page = FindLeaf(key, Operation::DELETE, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 执行删除,说要删除的节点不存在与树中，结束删除
  if (node->GetSize() == node->RemoveAndDeleteRecord(key, comparator_)) {
    // 解锁
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }
  // 检查删除后的节点是否需要进一步操作，合并或重分配
  auto node_should_delete = CoalesceOrRedistribute(node, transaction);
  leaf_page->WUnlatch();
  if (node_should_delete) {
    return;
  }
}
/**
 * 执行分裂和合并算法
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // 如果当前节点为根节点，则处理根节点
  if (node->IsRootPage()) {
    auto root_should_delete = AdjustRoot(node);
    return root_should_delete;
  }
  // 如果当前节点的数量，大于最大数量的一半，则不需要执行合并
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  // 其他情况，合并或者重分配
  // 首先获取父节点
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page);
  auto index = parent_node->IndexAt(node->GetPageId());  // 查找该节点在父节点中的位置
  // 如果当前节点不是第一个，从左兄弟节点中借一个
  if (index > 0) {
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index - 1));
    sibling_page->WLatch();
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    // 如果兄弟节点的的孩子大于一半，则进行重分配->从兄弟节点中借一个元素
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      Redistribute(sibling_node, node, parent_node, index, true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;
    }
    // 执行合并
    auto parent_node_should_delete = Coalesce(sibling_node, node, parent_node, index, transaction);
    if (parent_node_should_delete) {
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }
  if (index != parent_node->GetSize() - 1) {
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index + 1));
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    // 节点为失衡
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      Redistribute(sibling_node, node, parent_node, index, false);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;
    }
    // 合并
    auto parent_node_should_delete = Coalesce(sibling_node, node, parent_node, index, transaction);
    if (parent_node_should_delete) {
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  return false;
}

/**
 * 调整根节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::AdjustRoot(N *old_root_node) -> bool {
  // 判断，根节点是否为叶子节点，根节点仅有一个子节点时，进行调整
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    auto only_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
    auto *only_child_node = reinterpret_cast<InternalPage *>(only_child_page->GetData());
    root_page_id_ = only_child_node->GetPageId();  // 将孩子置为新的根
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(only_child_node->GetPageId(), true);  // 解除绑定
    return true;                                                          // 将老根节点设置为可删除节点
  }
  // 如果节点为空时，清除根节点，将根节点id置为无效
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}

/**
 * 重分配-从兄弟节点中借一个节点，维持当前节点的节点数
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                                  bool from_prev) {
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    // 兄弟节点在左边，拿兄弟节点的最后一个到该节点做补充
    if (from_prev) {
      neighbor_leaf_node->MoveLastToFirst(leaf_node);  // 从后面拿一个放在当前节点的第一个
      parent->SetKeyAt(index, leaf_node->KeyAt(0));    // 替换掉当前父节点的key值
    } else {
      neighbor_leaf_node->MoveFirstToLast(leaf_node);  // 从兄弟节点的第一个拿到当前节点
      parent->SetKeyAt(index + 1, neighbor_leaf_node->KeyAt(0));
    }
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (from_prev) {
      neighbor_internal_node->MoveLastToFirst(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    } else {
      neighbor_internal_node->MoveFirstToLast(internal_node, parent->KeyAt(index + 1), buffer_pool_manager_);
      parent->SetKeyAt(index, neighbor_internal_node->KeyAt(0));
    }
  }
}
/**
 * 合并两个节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) -> bool {
  auto middle_key = parent->KeyAt(index);
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf_node);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
  }
  parent->Remove(index);
  return CoalesceOrRedistribute(parent, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator  无输入，首先找到最左边的left page，构造index迭代器
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leftmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator 输入一个最小的key，首先发现包含输入key的leaf page ，然后构造index迭代器
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leaf_page = FindLeaf(key, Operation::SEARCH);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
}
/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node 构造迭代器代表着在left node中最后的一个键值对
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto rightmost_page = FindLeaf(KeyType(), Operation::SEARCH, nullptr, false, true);
  auto *leaf_node = reinterpret_cast<LeafPage *>(rightmost_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_page, leaf_node->GetSize());
}
/**
 * @return Page id of the root of this tree 返回这颗树根节点的page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
