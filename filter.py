"""
内容过滤模块
去重、黑名单、质量过滤、匹配度评分
"""
import logging
from typing import Optional

from pixiv_client import Illust
import database as db

logger = logging.getLogger(__name__)


def calculate_match_score(illust: Illust, xp_profile: dict[str, float]) -> float:
    """
    计算作品与 XP 画像的匹配度（改进版）
    
    算法:
    1. 累加匹配 tag 的权重
    2. 按最高权重归一化
    3. 奖励高权重匹配（Top 20% 的 tag 匹配额外 +20%）
    4. 使用对数平滑匹配数量影响
    
    Returns:
        0.0 ~ 1.0 归一化分数
    """
    import math
    
    if not illust.tags or not xp_profile:
        return 0.0
    
    # 获取 XP 中的最大权重和 Top 20% 阈值
    sorted_weights = sorted(xp_profile.values(), reverse=True)
    max_weight = sorted_weights[0] if sorted_weights else 1.0
    top_threshold = sorted_weights[len(sorted_weights) // 5] if len(sorted_weights) >= 5 else max_weight * 0.8
    
    total_score = 0.0
    matched_count = 0
    high_weight_matches = 0
    
    for tag in illust.tags:
        # 使用统一的归一化逻辑
        from utils import normalize_tag
        normalized_tag = normalize_tag(tag)
        
        weight = None
        if normalized_tag in xp_profile:
            weight = xp_profile[normalized_tag]
        # Fallback: 尝试原始Tag的小写 (有些特例可能未被归一化覆盖)
        elif tag.lower() in xp_profile:
            weight = xp_profile[tag.lower()]
        
        if weight is not None:
            total_score += weight
            matched_count += 1
            if weight >= top_threshold:
                high_weight_matches += 1
    
    if matched_count == 0:
        return 0.0
    
    # 基础分：权重总和 / (匹配数 × 最大权重)
    base_score = total_score / (matched_count * max_weight) if max_weight > 0 else 0.0
    
    # 匹配数量奖励：log(1 + n) / log(6) → 匹配5个以上趋于饱和
    quantity_bonus = min(math.log(1 + matched_count) / math.log(6), 0.3)
    
    # 高权重匹配奖励：每匹配一个 Top 20% 的 tag +5%，最多 +20%
    quality_bonus = min(high_weight_matches * 0.05, 0.2)
    
    return min(base_score + quantity_bonus + quality_bonus, 1.0)


class ContentFilter:
    """内容过滤器"""
    
    def __init__(
        self,
        blacklist_tags: Optional[list[str]] = None,
        daily_limit: int = 20,
        exclude_ai: bool = True,
        min_match_score: float = 0.0,
        match_weight: float = 0.5,
        max_per_artist: int = 3,
        subscribed_artists: Optional[list[int]] = None,  # 关注的画师 ID
        artist_boost: float = 0.3,  # 关注画师的匹配度加成
        min_create_days: int = 0,  # 过滤 N 天前的老图 (0=不过滤)
        r18_mode: bool = False  # 涩涩模式：只推送 R-18
    ):
        self.blacklist_tags = set(t.lower() for t in (blacklist_tags or []))
        self.daily_limit = daily_limit
        self.exclude_ai = exclude_ai
        self.min_match_score = min_match_score
        self.match_weight = match_weight
        self.max_per_artist = max_per_artist
        self.subscribed_artists = set(subscribed_artists or [])
        self.artist_boost = artist_boost
        self.min_create_days = min_create_days
        self.r18_mode = r18_mode
        
        # 硬性过滤Tag
        self.blacklist_tags.update({"r-18g", "guro", "gore"})
    
    async def filter(
        self,
        illusts: list[Illust],
        xp_profile: Optional[dict[str, float]] = None
    ) -> list[Illust]:
        """
        过滤管道
        
        1. 去重（已推送）
        2. 时间过滤（老图片）
        3. 硬性过滤（R-18G、AI）
        4. 黑名单Tag
        5. 匹配度过滤 + 画师权重加成
        6. 综合排序
        7. 多样性控制
        8. 每日上限
        """
        from datetime import datetime, timedelta
        
        if not illusts:
            return []
        
        # 计算时间阈值
        time_threshold = None
        if self.min_create_days > 0:
            time_threshold = datetime.now(illusts[0].create_date.tzinfo if illusts else None) - timedelta(days=self.min_create_days)
        
        result = []
        filtered_by_time = 0
        
        for illust in illusts:
            # 1. 去重
            if await db.is_pushed(illust.id):
                continue
            
            # 2. 时间过滤
            if time_threshold and illust.create_date < time_threshold:
                filtered_by_time += 1
                continue
            
            # 3. R-18G 排除
            if self._has_blacklisted_tag(illust):
                continue
            
            # 4. AI 生成排除
            if self.exclude_ai and illust.ai_type == 2:
                continue
            
            # 4.1 涩涩模式 (R-18 Mode Control)
            # 支持 bool (旧配置) 和 str (新配置: safe, mixed, r18_only)
            mode_str = str(self.r18_mode).lower()
            
            if mode_str in ("true", "r18_only", "pure"):
                # 纯 18+ 模式：只允许 R-18
                if not illust.is_r18:
                    continue
            elif mode_str in ("safe", "18-", "clean"):
                # 净网模式：禁止 R-18
                if illust.is_r18:
                    continue
            else:
                # 默认/mixed/neutral：不因 R-18 属性过滤，全凭匹配度
                pass
            
            result.append(illust)
        
        if filtered_by_time > 0:
            logger.debug(f"过滤 {filtered_by_time} 个超过 {self.min_create_days} 天的老图")
        
        # 去重（同批次内）
        seen_ids = set()
        unique_result = []
        for illust in result:
            if illust.id not in seen_ids:
                seen_ids.add(illust.id)
                unique_result.append(illust)
        
        # 5. 计算匹配度并过滤 + 画师权重加成
        scored_result = []
        for illust in unique_result:
            if xp_profile:
                score = calculate_match_score(illust, xp_profile)
                
                # 画师权重加成：关注画师的作品额外加成
                if illust.user_id in self.subscribed_artists:
                    score = min(score + self.artist_boost, 1.0)
                
                if score < self.min_match_score:
                    continue
            else:
                score = 0.0
                # 无 XP 时，关注画师也给予基础分
                if illust.user_id in self.subscribed_artists:
                    score = self.artist_boost
            
            scored_result.append((illust, score))
        
        # 6. 综合排序：match_score * weight + normalized_bookmark * (1-weight)
        if scored_result:
            max_bookmark = max(item[0].bookmark_count for item in scored_result) or 1
            
            def sort_key(item):
                illust, score = item
                normalized_bookmark = illust.bookmark_count / max_bookmark
                return score * self.match_weight + normalized_bookmark * (1 - self.match_weight)
            
            scored_result.sort(key=sort_key, reverse=True)
        
        # 构建 illust -> score 的映射
        score_map = {item[0].id: item[1] for item in scored_result}
        sorted_illusts = [item[0] for item in scored_result]
        
        # 6. 多样性控制：限制每个画师的作品数
        artist_count = {}
        diverse_result = []
        for illust in sorted_illusts:
            count = artist_count.get(illust.user_id, 0)
            if count < self.max_per_artist:
                # 将匹配度附加到对象上（动态属性）
                illust.match_score = score_map.get(illust.id, 0.0)
                diverse_result.append(illust)
                artist_count[illust.user_id] = count + 1
        
        # 7. 每日上限
        final_result = diverse_result[:self.daily_limit]
        
        # 记录匹配度日志
        if xp_profile and scored_result:
            top_3 = scored_result[:3]
            log_items = [f"{i[0].title[:10]}(score={i[1]:.2f})" for i in top_3]
            logger.info(f"匹配度 Top3: {', '.join(log_items)}")
        
        logger.info(f"过滤后剩余 {len(final_result)} 个作品 (涉及 {len(artist_count)} 个画师)")
        return final_result
    
    def check_illust(self, illust: Illust) -> bool:
        """检查单个作品是否满足基本过滤条件 (Blacklist, AI, R18, Time)"""
        # 1. 基础有效性
        if not illust.id: return False
        
        # 2. 时间过滤 (如果配置)
        if self.min_create_days > 0:
            from datetime import datetime, timedelta
            time_threshold = datetime.now(illust.create_date.tzinfo) - timedelta(days=self.min_create_days)
            if illust.create_date < time_threshold:
                return False

        # 3. R-18G / Blacklist
        if self._has_blacklisted_tag(illust):
            return False
            
        # 4. AI 排除
        if self.exclude_ai and illust.ai_type == 2:
            return False
            
        # 5. R-18 Mode
        mode_str = str(self.r18_mode).lower()
        if mode_str in ("true", "r18_only", "pure"):
            if not illust.is_r18: return False
        elif mode_str in ("safe", "18-", "clean"):
            if illust.is_r18: return False
            
        return True

    def _has_blacklisted_tag(self, illust: Illust) -> bool:
        """检查是否包含黑名单Tag"""
        for tag in illust.tags:
            if tag.lower() in self.blacklist_tags:
                return True
        return False
    
    async def add_to_blacklist(self, tag: str):
        """动态添加黑名单Tag"""
        self.blacklist_tags.add(tag.lower())
        logger.info(f"Tag '{tag}' 已加入黑名单")
